package plumber

import (
	"context"
	"fmt"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/mcuadros/go-lookup"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/actions"
	"github.com/batchcorp/plumber/bus"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/validate"
)

var (
	ErrMissingShutdownCtx      = errors.New("ServiceShutdownCtx cannot be nil")
	ErrMissingMainShutdownFunc = errors.New("MainShutdownFunc cannot be nil")
	ErrMissingMainContext      = errors.New("MainContext cannot be nil")
	ErrMissingOptions          = errors.New("CLIOptions cannot be nil")
	ErrMissingPersistentConfig = errors.New("PersistentConfig cannot be nil")
	ErrMissingKongCtx          = errors.New("KongCtx cannot be nil")
	ErrMissingActions          = errors.New("Actions cannot be nil")
)

// Config contains configurable options for instantiating a new Plumber
type Config struct {
	PersistentConfig   *config.Config
	Actions            actions.IActions
	ServiceShutdownCtx context.Context
	MainShutdownFunc   context.CancelFunc
	MainShutdownCtx    context.Context
	CLIOptions         *opts.CLIOptions
	KongCtx            *kong.Context
}

type Plumber struct {
	*Config

	Bus     bus.IBus
	RelayCh chan interface{}

	cliMD       map[pb.MDType]*desc.MessageDescriptor
	cliConnOpts *opts.ConnectionOptions

	log *logrus.Entry
}

// New instantiates a properly configured instance of Plumber or a config error
func New(cfg *Config) (*Plumber, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	p := &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		log:     logrus.WithField("pkg", "plumber"),
	}

	// If backend is filled out, we are in CLI-mode, performing an action that
	// will need a connection and possibly a pb message descriptor.
	if cfg.CLIOptions.Global.XBackend != "" {
		mds, err := GenerateMessageDescriptors(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
		}

		connCfg, err := generateConnectionOptions(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate connection config")
		}

		p.cliMD = mds
		p.cliConnOpts = connCfg
	}

	return p, nil
}

// generateConnectionOptions generates a connection config from passed in CLI
// options. This function is used by plumber in CLI mode.
func generateConnectionOptions(cfg *opts.CLIOptions) (*opts.ConnectionOptions, error) {
	if cfg == nil {
		return nil, errors.New("cli options config cannot be nil")
	}

	connCfg := &opts.ConnectionOptions{
		Name:  "plumber-cli",
		Notes: "Generated via plumber-cli",

		// This is what we'll be trying to create dynamically:
		//Conn: &opts.ConnectionOptions_Kafka{
		//	Kafka: &args.KafkaConn{},
		//},
	}

	// Some backends have a dash, remove it
	backendName := strings.Replace(cfg.Global.XBackend, "-", "", -1)

	// We are looking for the individual conn located at: cfg.$action.$backendName.XConn
	lookupStrings := []string{cfg.Global.XAction, backendName, "XConn"}

	backendInterface, err := lookup.LookupI(cfg, lookupStrings...)
	if err != nil {
		return nil, fmt.Errorf("unable to lookup connection info for backendName '%s': %s",
			cfg.Global.XBackend, err)
	}

	conn, ok := opts.GenerateConnOpts(backendName, backendInterface.Interface())
	if !ok {
		return nil, errors.New("unable to generate connection options via proto func")
	}

	connCfg.Conn = conn

	return connCfg, nil
}

// Run is the main entrypoint to the plumber application
func (p *Plumber) Run() {
	var err error

	switch p.CLIOptions.Global.XAction {
	case "server":
		err = p.RunServer()
	case "batch":
		err = p.HandleBatchCmd()
	case "read":
		err = p.HandleReadCmd()
	case "write":
		err = p.HandleWriteCmd()
	case "relay":
		printer.PrintRelayOptions(p.CLIOptions)
		err = p.HandleRelayCmd()
	case "tunnel":
		logrus.Fatal("tunnel mode not implemented")
		//err = p.HandleTunnelCmd()
	default:
		logrus.Fatalf("unrecognized command: %s", p.CLIOptions.Global.XAction)
	}

	if err != nil {
		logrus.Fatalf("unable to complete command: %s", err)
	}
}

func GenerateMessageDescriptors(cliOpts *opts.CLIOptions) (map[pb.MDType]*desc.MessageDescriptor, error) {
	if cliOpts.Read != nil && cliOpts.Read.DecodeOptions != nil {
		return generateMessageDescriptorsRead(cliOpts)
	}

	if cliOpts.Write != nil && cliOpts.Write.EncodeOptions != nil {
		return generateMessageDescriptorsWrite(cliOpts)
	}

	return nil, nil
}

func generateMessageDescriptorsWrite(cliOpts *opts.CLIOptions) (map[pb.MDType]*desc.MessageDescriptor, error) {
	if cliOpts.Write.EncodeOptions.EncodeType != encoding.EncodeType_ENCODE_TYPE_JSONPB {
		return nil, nil
	}

	descriptors := make(map[pb.MDType]*desc.MessageDescriptor)

	logrus.Debug("attempting to find encoding protobuf descriptors")

	pbSettings := cliOpts.Write.EncodeOptions.ProtobufSettings
	pbDirs := pbSettings.ProtobufDirs
	pbRootMessage := pbSettings.ProtobufRootMessage

	if err := validate.ProtobufOptionsForCLI(pbDirs, pbRootMessage); err != nil {
		return nil, errors.Wrap(err, "unable to validate protobuf settings for encode")
	}

	envelopeMD, err := pb.FindMessageDescriptor(pbDirs, pbRootMessage)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find root message descriptor for encode")
	}
	descriptors[pb.MDEnvelope] = envelopeMD

	if pbSettings.ProtobufEnvelopeType == encoding.EnvelopeType_ENVELOPE_TYPE_SHALLOW {
		payloadMD, err := pb.FindMessageDescriptor(pbDirs, pbSettings.ShallowEnvelopeMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to find shallow envelope payload message descriptor for decode")
		}
		descriptors[pb.MDPayload] = payloadMD
	}

	return descriptors, nil
}

func generateMessageDescriptorsRead(cliOpts *opts.CLIOptions) (map[pb.MDType]*desc.MessageDescriptor, error) {
	descriptors := make(map[pb.MDType]*desc.MessageDescriptor)

	if cliOpts.Read.DecodeOptions.DecodeType != encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		return nil, nil
	}

	logrus.Debug("attempting to find decoding protobuf descriptors")

	pbSettings := cliOpts.Read.DecodeOptions.ProtobufSettings
	pbDirs := pbSettings.ProtobufDirs
	pbRootMessage := pbSettings.ProtobufRootMessage

	if err := validate.ProtobufOptionsForCLI(pbDirs, pbRootMessage); err != nil {
		return nil, errors.Wrap(err, "unable to validate protobuf settings for decode")
	}

	md, err := pb.FindMessageDescriptor(pbDirs, pbRootMessage)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find root message descriptor for decode")
	}
	descriptors[pb.MDEnvelope] = md

	if pbSettings.ProtobufEnvelopeType == encoding.EnvelopeType_ENVELOPE_TYPE_SHALLOW {
		payloadMD, err := pb.FindMessageDescriptor(pbDirs, pbSettings.ShallowEnvelopeMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to find shallow envelope payload message descriptor for decode")
		}
		descriptors[pb.MDPayload] = payloadMD
	}

	return descriptors, nil
}

// validateConfig ensures all correct values for Config are passed
func validateConfig(cfg *Config) error {
	if cfg.ServiceShutdownCtx == nil {
		return ErrMissingShutdownCtx
	}

	if cfg.CLIOptions == nil {
		return ErrMissingOptions
	}

	if cfg.PersistentConfig == nil {
		return ErrMissingPersistentConfig
	}

	if cfg.KongCtx == nil {
		return ErrMissingKongCtx
	}

	if cfg.Actions == nil {
		return ErrMissingActions
	}

	return nil
}

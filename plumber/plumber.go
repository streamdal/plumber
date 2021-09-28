package plumber

import (
	"context"
	"fmt"

	"github.com/batchcorp/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/jhump/protoreflect/desc"
	"github.com/mcuadros/go-lookup"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/validate"
)

var (
	ErrMissingShutdownCtx      = errors.New("ServiceShutdownCtx cannot be nil")
	ErrMissingMainShutdownFunc = errors.New("MainShutdownFunc cannot be nil")
	ErrMissingMainContext      = errors.New("MainContext cannot be nil")
	ErrMissingOptions          = errors.New("CLIOptions cannot be nil")
)

// Config contains configurable options for instantiating a new Plumber
type Config struct {
	PersistentConfig   *config.Config
	ServiceShutdownCtx context.Context
	MainShutdownFunc   context.CancelFunc
	MainShutdownCtx    context.Context
	CLIOptions         *opts.CLIOptions
	KongCtx            *kong.Context
}

type Plumber struct {
	*Config

	Etcd    *etcd.Etcd
	RelayCh chan interface{}

	cliMD       *desc.MessageDescriptor
	cliConnOpts *opts.ConnectionOptions
	log         *logrus.Entry
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
		md, err := GenerateMessageDescriptor(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
		}

		connCfg, err := generateConnectionOptions(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate connection config")
		}

		p.cliMD = md
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

	// We are looking for the individual conn located at: cfg.$action.$backendName.XConn
	lookupStrings := []string{cfg.Global.XAction, cfg.Global.XBackend, "XConn"}

	rvKafkaConn, err := lookup.LookupI(cfg, lookupStrings...)
	if err != nil {
		return nil, fmt.Errorf("unable to lookup connection info for backend '%s': %s",
			cfg.Global.XBackend, err)
	}

	conn, ok := opts.GenerateConnOpts(cfg.Global.XBackend, rvKafkaConn.Interface())
	if !ok {
		return nil, errors.New("unable to generate connection options via proto func")
	}

	connCfg.Conn = conn

	return connCfg, nil
}

// TODO: This function should be auto-generated (as part of `make generate/all`
func generateGenericConnOpts(backend string, connArgs interface{}) (opts.IsConnectionOptions_Conn, bool) {
	switch backend {
	case "kafka":
		asserted, ok := connArgs.(args.KafkaConn)
		if !ok {
			return nil, false
		}

		return &opts.ConnectionOptions_Kafka{
			Kafka: &asserted,
		}, true
	}

	return nil, false
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
	case "dynamic":
		err = p.HandleDynamicCmd()
	default:
		logrus.Fatalf("unrecognized command: %s", p.CLIOptions.Global.XAction)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func GenerateMessageDescriptor(cliOpts *opts.CLIOptions) (*desc.MessageDescriptor, error) {
	if cliOpts.Read != nil && cliOpts.Read.DecodeOptions != nil {
		if cliOpts.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {
			logrus.Debug("attempting to find decoding protobuf descriptors")

			pbDirs := cliOpts.Read.DecodeOptions.ProtobufSettings.ProtobufDirs
			pbRootMessage := cliOpts.Read.DecodeOptions.ProtobufSettings.ProtobufRootMessage

			if err := validate.ProtobufOptionsForCLI(pbDirs, pbRootMessage); err != nil {
				return nil, errors.Wrap(err, "unable to validate protobuf settings for decode")
			}

			md, err := pb.FindMessageDescriptor(pbDirs, pbRootMessage)
			if err != nil {
				return nil, errors.Wrap(err, "unable to find root message descriptor for decode")
			}

			return md, nil
		}

	}

	if cliOpts.Write != nil && cliOpts.Write.EncodeOptions != nil {
		if cliOpts.Write != nil && cliOpts.Write.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
			logrus.Debug("attempting to find encoding protobuf descriptors")

			pbDirs := cliOpts.Write.EncodeOptions.ProtobufSettings.ProtobufDirs
			pbRootMessage := cliOpts.Write.EncodeOptions.ProtobufSettings.ProtobufRootMessage

			if err := validate.ProtobufOptionsForCLI(pbDirs, pbRootMessage); err != nil {
				return nil, errors.Wrap(err, "unable to validate protobuf settings for encode")
			}

			md, err := pb.FindMessageDescriptor(pbDirs, pbRootMessage)
			if err != nil {
				return nil, errors.Wrap(err, "unable to find root message descriptor for encode")
			}

			return md, nil
		}
	}

	return nil, nil
}

// validateConfig ensures all correct values for Config are passed
func validateConfig(cfg *Config) error {
	if cfg.ServiceShutdownCtx == nil {
		return ErrMissingShutdownCtx
	}

	if cfg.CLIOptions == nil {
		return ErrMissingOptions
	}

	if cfg.MainShutdownCtx == nil {
		return ErrMissingMainContext
	}

	if cfg.MainShutdownFunc == nil {
		return ErrMissingMainShutdownFunc
	}

	return nil
}

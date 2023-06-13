package plumber

import (
	"context"
	"fmt"
	"strings"
	"time"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/mcuadros/go-lookup"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/telemetry"

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
	ErrMissingTelemetry        = errors.New("Telemetry cannot be nil")
)

const (
	// WasmUpdateInterval is how often we check for wasm file updates
	// Checks will be ignored if no wasm files are present in the config, indicating they are not currently needed
	WasmUpdateInterval = time.Hour * 6
)

// Config contains configurable options for instantiating a new Plumber
type Config struct {
	Telemetry          telemetry.ITelemetry
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

	cliFDS      *dpb.FileDescriptorSet
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
		fds, err := GenerateMessageDescriptors(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
		}

		var connCfg *opts.ConnectionOptions

		// 'manage' does not utilize connection options (and has a slightly diff
		// syntax from other CLI actions); due to this, we avoid generating them
		// altogether.
		if cfg.CLIOptions.Global.XAction != "manage" {
			connCfg, err = generateConnectionOptions(cfg.CLIOptions)
		}

		if err != nil {
			return nil, errors.Wrap(err, "unable to dynamic options config")
		}

		p.cliFDS = fds
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
	case "streamdal":
		err = p.HandleStreamdalCmd()
	case "read":
		err = p.HandleReadCmd()
	case "write":
		err = p.HandleWriteCmd()
	case "relay":
		printer.PrintRelayOptions(p.CLIOptions)
		err = p.HandleRelayCmd()
	case "tunnel":
		err = p.HandleTunnelCmd()
	case "manage":
		err = p.HandleManageCmd()
	default:
		logrus.Fatalf("unrecognized command: %s", p.CLIOptions.Global.XAction)
	}

	if err != nil {
		logrus.Fatalf("unable to complete command: %s", err)
	}
}

func GenerateMessageDescriptors(cliOpts *opts.CLIOptions) (*dpb.FileDescriptorSet, error) {
	if cliOpts.Read != nil && cliOpts.Read.DecodeOptions != nil {
		return generateFileDescriptorsRead(cliOpts)
	}

	if cliOpts.Write != nil && cliOpts.Write.EncodeOptions != nil {
		return generateFileDescriptorsWrite(cliOpts)
	}

	return nil, nil
}

func generateFileDescriptorsWrite(cliOpts *opts.CLIOptions) (*dpb.FileDescriptorSet, error) {
	if cliOpts.Write.EncodeOptions.EncodeType != encoding.EncodeType_ENCODE_TYPE_JSONPB {
		return nil, nil
	}

	pbSettings := cliOpts.Write.EncodeOptions.ProtobufSettings
	fdsFile := pbSettings.ProtobufDescriptorSet
	pbDirs := pbSettings.ProtobufDirs
	pbRootMessage := pbSettings.ProtobufRootMessage

	if err := validate.ProtobufOptionsForCLI(pbDirs, pbRootMessage, fdsFile); err != nil {
		return nil, errors.Wrap(err, "unable to validate protobuf settings for encode")
	}

	return pb.ProcessDescriptors(pbDirs, fdsFile)
}

func generateFileDescriptorsRead(cliOpts *opts.CLIOptions) (*dpb.FileDescriptorSet, error) {
	if cliOpts.Read.DecodeOptions.DecodeType != encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		return nil, nil
	}

	pbSettings := cliOpts.Read.DecodeOptions.ProtobufSettings
	fdsFile := pbSettings.ProtobufDescriptorSet
	pbDirs := pbSettings.ProtobufDirs
	pbRootMessage := pbSettings.ProtobufRootMessage

	if err := validate.ProtobufOptionsForCLI(pbDirs, pbRootMessage, fdsFile); err != nil {
		return nil, errors.Wrap(err, "unable to validate protobuf settings for encode")
	}

	return pb.ProcessDescriptors(pbDirs, fdsFile)
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

	if cfg.PersistentConfig.EnableTelemetry {
		if cfg.Telemetry == nil {
			return ErrMissingTelemetry
		}
	}

	return nil
}

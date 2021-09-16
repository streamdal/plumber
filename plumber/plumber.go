package plumber

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alecthomas/kong"
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

		connCfg, err := GenerateConnectionConfig(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate connection config")
		}

		p.cliMD = md
		p.cliConnOpts = connCfg
	}

	return p, nil
}

// GenerateConnectionConfig generates a connection config from passed in CLI
// options. This function is used by plumber in CLI mode.
func GenerateConnectionConfig(cfg *opts.CLIOptions) (*opts.ConnectionOptions, error) {
	if cfg == nil {
		return nil, errors.New("cli options config cannot be nil")
	}

	connCfg := &opts.ConnectionOptions{
		Name:  "plumber-cli",
		Notes: "Generated via plumber-cli",
	}

	// We are looking for the individual conn located at: cfg.$action.$backendName.XConn
	lookupStrings := []string{cfg.Global.XAction, cfg.Global.XBackend, "XConn"}

	var value reflect.Value
	var err error

	value, err = lookup.LookupI(cfg, lookupStrings...)
	if err != nil {
		return nil, fmt.Errorf("unable to lookup connection info for backend '%s': %s",
			cfg.Global.XBackend, err)
	}

	// This is a little funky - after finding the conn, it'll have an interface{}
	// type, so we assert it to a connection config so we can perform an assignment.
	assertedConnCfg, ok := value.Interface().(opts.IsConnectionOptions_Conn)
	if !ok {
		return nil, errors.New("unable to type assert connection config")
	}

	connCfg.Conn = assertedConnCfg

	return connCfg, nil
}

// Run is the main entrypoint to the plumber application
func (p *Plumber) Run() {
	var err error

	switch p.CLIOptions.Global.XAction {
	case "server": // DONE
		err = p.RunServer()
	case "batch":
		err = p.HandleBatchCmd() // TODO: Update
	case "read": // DONE
		err = p.HandleReadCmd()
	case "write": // DONE
		err = p.HandleWriteCmd()
	case "relay": // DONE
		printer.PrintRelayOptions(p.CLIOptions)
		err = p.HandleRelayCmd() // DONE
	case "dynamic":
		err = p.HandleDynamicCmd() // DONE
	default:
		logrus.Fatalf("unrecognized command: %s", p.CLIOptions.Global.XAction)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func GenerateMessageDescriptor(cliOpts *opts.CLIOptions) (*desc.MessageDescriptor, error) {
	if cliOpts.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_JSONPB ||
		cliOpts.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {

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

	if cliOpts.Write.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
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

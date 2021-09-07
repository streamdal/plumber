package plumber

import (
	"context"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/validate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
)

var (
	ErrMissingShutdownCtx      = errors.New("ServiceShutdownCtx cannot be nil")
	ErrMissingMainShutdownFunc = errors.New("MainShutdownFunc cannot be nil")
	ErrMissingMainContext      = errors.New("MainContext cannot be nil")
	ErrMissingOptions          = errors.New("Options cannot be nil")
)

// Config contains configurable options for instantiating a new Plumber
type Config struct {
	PersistentConfig   *config.Config
	ServiceShutdownCtx context.Context
	MainShutdownFunc   context.CancelFunc
	MainShutdownCtx    context.Context
	Options            *protos.CLIOptions
	KongCtx            *kong.Context
}

type Plumber struct {
	*Config
	Etcd    *etcd.Etcd
	RelayCh chan interface{}
	log     *logrus.Entry
}

// New instantiates a properly configured instance of Plumber or a config error
func New(cfg *Config) (*Plumber, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	if err := MaybePopulateMDs(cfg.KongCtx, cfg.Options); err != nil {
		return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
	}

	return &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		log:     logrus.WithField("pkg", "plumber"),
	}, nil
}

// Run is the main entrypoint to the plumber application
func (p *Plumber) Run() {
	var err error

	switch {
	case p.KongCtx == "server":
		err = p.RunServer()
	case strings.HasPrefix(p.KongCtx, "batch"):
		err = p.HandleBatchCmd()
	case strings.HasPrefix(p.KongCtx, "read"):
		err = p.HandleReadCmd()
	case strings.HasPrefix(p.KongCtx, "write"):
		err = p.HandleWriteCmd()
	case strings.HasPrefix(p.KongCtx, "relay"):
		printer.PrintRelayOptions(p.KongCtx, p.Options)
		err = p.HandleRelayCmd()
	case strings.HasPrefix(p.KongCtx, "dynamic"):
		err = p.HandleDynamicCmd()
	case strings.HasPrefix(p.KongCtx, "lag"):
		err = p.HandleLagCmd()
	case strings.HasPrefix(p.KongCtx, "github"):
		err = p.HandleGithubCmd()
	default:
		logrus.Fatalf("unrecognized command: %s", p.KongCtx)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func MaybePopulateMDs(cmd string, opts *options.Options) error {
	if !strings.HasPrefix(cmd, "read") &&
		!strings.HasPrefix(cmd, "write") &&
		!strings.HasPrefix(cmd, "relay") {
		return nil
	}

	// If anything protobuf related is specified - we are using it!
	if opts.Decoding.ProtobufRootMessage != "" || len(opts.Decoding.ProtobufDirs) != 0 {
		logrus.Debug("attempting to find decoding protobuf descriptors")

		if err := validate.ProtobufOptions(
			opts.Decoding.ProtobufDirs,
			opts.Decoding.ProtobufRootMessage,
		); err != nil {
			return errors.Wrap(err, "unable to validate protobuf encode options")
		}

		md, err := pb.FindMessageDescriptor(opts.Decoding.ProtobufDirs, opts.Decoding.ProtobufRootMessage)
		if err != nil {
			return errors.Wrap(err, "unable to find root message descriptor")
		}

		opts.Decoding.MsgDesc = md
	}

	// If plumber is expected to ingest jsonpb's - protobuf options MUST be specified
	if opts.Write.InputType == "jsonpb" {
		logrus.Debug("attempting to find encoding protobuf descriptors")

		if err := validate.ProtobufOptions(
			opts.Encoding.ProtobufDirs,
			opts.Encoding.ProtobufRootMessage,
		); err != nil {
			return errors.Wrap(err, "unable to validate protobuf encode options")
		}

		md, err := pb.FindMessageDescriptor(opts.Encoding.ProtobufDirs, opts.Encoding.ProtobufRootMessage)
		if err != nil {
			return errors.Wrap(err, "unable to find root message descriptor")
		}

		opts.Encoding.MsgDesc = md
	}

	return nil
}

// validateConfig ensures all correct values for Config are passed
func validateConfig(cfg *Config) error {
	if cfg.ServiceShutdownCtx == nil {
		return ErrMissingShutdownCtx
	}

	if cfg.Options == nil {
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

package plumber

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/pb"
	"github.com/jhump/protoreflect/desc"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/relay"
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
	Options            *options.Options
	Cmd                string
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

	if err := maybePopulateMDs(cfg.Cmd, cfg.Options); err != nil {
		return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
	}

	return &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		log:     logrus.WithField("pkg", "plumber"),
	}, nil
}

func maybePopulateMDs(cmd string, opts *options.Options) error {
	if !strings.HasPrefix(cmd, "read") &&
		!strings.HasPrefix(cmd, "write") &&
		!strings.HasPrefix(cmd, "relay") {
		return nil
	}

	// If anything protobuf related is specified - we are using it!
	if opts.Decoding.ProtobufRootMessage != "" || len(opts.Decoding.ProtobufDirs) != 0 {
		if err := options.ValidateProtobufOptions(
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
		if err := options.ValidateProtobufOptions(
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

// Run is the main entrypoint to the plumber application
func (p *Plumber) Run() {
	var err error

	switch {
	case p.Cmd == "server":
		err = p.RunServer()
	case strings.HasPrefix(p.Cmd, "batch"): // TODO: Update
		err = p.handleBatchCmd()
	case strings.HasPrefix(p.Cmd, "read"): // TODO: Update (in-progress)
		err = p.handleReadCmd()
	case strings.HasPrefix(p.Cmd, "write"): // TODO: Update
		err = p.handleWriteCmd()
	case strings.HasPrefix(p.Cmd, "relay"): // TODO: Update
		printer.PrintRelayOptions(p.Cmd, p.Options)
		err = p.handleRelayCmd()
	case strings.HasPrefix(p.Cmd, "dynamic"): // TODO: Update
		err = p.handleDynamicCmd()
	case strings.HasPrefix(p.Cmd, "lag"): // TODO: Update
		err = p.handleLagCmd()
	case strings.HasPrefix(p.Cmd, "github"): // TODO: Update
		err = p.handleGithubCmd()
	default:
		logrus.Fatalf("unrecognized command: %s", p.Cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

// startRelayService starts relay workers which send relay messages to grpc-collector
func (p *Plumber) startRelayService() error {
	relayCfg := &relay.Config{
		Token:              p.Options.Relay.Token,
		GRPCAddress:        p.Options.Relay.GRPCAddress,
		NumWorkers:         p.Options.Relay.NumWorkers,
		Timeout:            p.Options.Relay.GRPCTimeout,
		RelayCh:            p.RelayCh,
		DisableTLS:         p.Options.Relay.GRPCDisableTLS,
		BatchSize:          p.Options.Relay.BatchSize,
		Type:               p.Options.Relay.Type,
		MainShutdownFunc:   p.MainShutdownFunc,
		ServiceShutdownCtx: p.ServiceShutdownCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(p.Options.Relay.HTTPListenAddress, p.Options.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	// Launch gRPC Relayer
	if err := grpcRelayer.StartWorkers(p.ServiceShutdownCtx); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	go grpcRelayer.WaitForShutdown()

	return nil
}

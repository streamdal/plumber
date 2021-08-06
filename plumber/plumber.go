package plumber

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
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
	Options            *cli.Options
	Cmd                string
}

type Plumber struct {
	*Config
	Etcd    *etcd.Etcd
	RelayCh chan interface{}
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

// New instantiates a properly configured instance Plumber, or configuration error
func New(cfg *Config) (*Plumber, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	md, err := getMd(cfg.Options)
	if err != nil {
		return nil, err
	}

	return &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		MsgDesc: md,
		log:     logrus.WithField("pkg", "plumber"),
	}, nil
}

// getMd retrieves a protobuf message descriptor if protobuf read/write flags are specified
func getMd(opts *cli.Options) (*desc.MessageDescriptor, error) {
	var err error
	var md *desc.MessageDescriptor

	// If anything protobuf-related is specified, it's being used
	if opts.ReadProtobufRootMessage != "" || len(opts.ReadProtobufDirs) != 0 {
		if err := cli.ValidateProtobufOptions(
			opts.ReadProtobufDirs,
			opts.ReadProtobufRootMessage,
		); err != nil {
			return nil, fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	if opts.WriteInputType == "jsonpb" {
		if err := cli.ValidateProtobufOptions(
			opts.WriteProtobufDirs,
			opts.WriteProtobufRootMessage,
		); err != nil {
			return nil, fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	if opts.WriteInputType == "jsonpb" {
		md, err = pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to find root message descriptor")
		}
	}

	if opts.ReadProtobufRootMessage != "" {
		md, err = pb.FindMessageDescriptor(opts.ReadProtobufDirs, opts.ReadProtobufRootMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to find root message descriptor")
		}
	}

	return md, err
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
	case strings.HasPrefix(p.Cmd, "batch"):
		err = p.parseBatchCmd()
	case strings.HasPrefix(p.Cmd, "read"):
		err = p.parseCmdRead()
	case strings.HasPrefix(p.Cmd, "write"):
		err = p.parseCmdWrite()
	case strings.HasPrefix(p.Cmd, "relay"):
		printer.PrintRelayOptions(p.Cmd, p.Options)
		err = p.parseCmdRelay()
	case strings.HasPrefix(p.Cmd, "dynamic"):
		err = p.parseCmdDynamic()
	case strings.HasPrefix(p.Cmd, "lag"):
		err = p.parseCmdLag()
	default:
		logrus.Fatalf("unrecognized command: %s", p.Cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

// startGRPCService starts relay workers which send relay messages to grpc-collector
func (p *Plumber) startGRPCService() error {
	relayCfg := &relay.Config{
		Token:              p.Options.RelayToken,
		GRPCAddress:        p.Options.RelayGRPCAddress,
		NumWorkers:         p.Options.RelayNumWorkers,
		Timeout:            p.Options.RelayGRPCTimeout,
		RelayCh:            p.RelayCh,
		DisableTLS:         p.Options.RelayGRPCDisableTLS,
		BatchSize:          p.Options.RelayBatchSize,
		Type:               p.Options.RelayType,
		MainShutdownFunc:   p.MainShutdownFunc,
		ServiceShutdownCtx: p.ServiceShutdownCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(p.Options.RelayHTTPListenAddress, p.Options.Version); err != nil {
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

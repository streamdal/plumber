package plumber

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/relay"
)

var (
	ErrMissingShutdownContext  = errors.New("ServiceShutdownContext cannot be nil")
	ErrMissingMainShutdownFunc = errors.New("MainShutdownFunc cannot be nil")
	ErrMissingMainContext      = errors.New("MainContext cannot be nil")
	ErrMissingOptions          = errors.New("Options cannot be nil")
)

// Config contains configurable options for instantiating a new Plumber
type Config struct {
	ServiceShutdownContext context.Context
	MainShutdownFunc       context.CancelFunc
	MainShutdownContext    context.Context
	Options                *cli.Options
	Cmd                    string
}

type Plumber struct {
	*Config
	RelayCh chan interface{}
	log     *logrus.Entry
}

// New instantiates a properly configured instance Plumber, or configuration error
func New(cfg *Config) (*Plumber, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	return &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		log:     logrus.WithField("pkg", "plumber"),
	}, nil
}

// validateConfig ensures all correct values for Config are passed
func validateConfig(cfg *Config) error {
	if cfg.ServiceShutdownContext == nil {
		return ErrMissingShutdownContext
	}

	if cfg.Options == nil {
		return ErrMissingOptions
	}

	if cfg.MainShutdownContext == nil {
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
	default:
		logrus.Fatalf("unrecognized command: %s", p.Cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func (p *Plumber) startGRPCService() error {
	relayCfg := &relay.Config{
		Token:                  p.Options.RelayToken,
		GRPCAddress:            p.Options.RelayGRPCAddress,
		NumWorkers:             p.Options.RelayNumWorkers,
		Timeout:                p.Options.RelayGRPCTimeout,
		RelayCh:                p.RelayCh,
		DisableTLS:             p.Options.RelayGRPCDisableTLS,
		BatchSize:              p.Options.RelayBatchSize,
		Type:                   p.Options.RelayType,
		MainShutdownFunc:       p.MainShutdownFunc,
		ServiceShutdownContext: p.ServiceShutdownContext,
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
	if err := grpcRelayer.StartWorkers(p.ServiceShutdownContext); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	go grpcRelayer.WaitForShutdown()

	return nil
}

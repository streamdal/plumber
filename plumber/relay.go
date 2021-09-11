package plumber

import (
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/relay"
)

// HandleRelayCmd handles CLI relay mode. Container/envar mode is handled by processEnvRelayFlags
func (p *Plumber) HandleRelayCmd() error {
	if err := validate.RelayOptions(p.CLIOptions.Relay); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	backend, err := backends.New(p.CLIOptions.Global.XBackend, p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	if err := p.startRelayService(); err != nil {
		return errors.Wrap(err, "unable to start relay service")
	}

	// Blocks until ctx is cancelled
	if err := backend.Relay(p.ServiceShutdownCtx, p.CLIOptions.Relay, p.RelayCh, nil); err != nil {
		return errors.Wrap(err, "unable to start relay backend")
	}

	p.log.Info("relay exiting")

	return nil
}

// startRelayService starts relay workers which send relay messages to grpc-collector
func (p *Plumber) startRelayService() error {
	relayCfg := &relay.Config{
		Token:              p.CLIOptions.Relay.CollectionToken,
		GRPCAddress:        p.CLIOptions.Relay.XBatchshGrpcAddress,
		NumWorkers:         p.CLIOptions.Relay.NumWorkers,
		Timeout:            util.DurationSec(p.CLIOptions.Relay.XBatchshGrpcTimeoutSeconds),
		RelayCh:            p.RelayCh,
		DisableTLS:         p.CLIOptions.Relay.XBatchshGrpcDisableTls,
		BatchSize:          p.CLIOptions.Relay.BatchSize,
		Type:               p.CLIOptions.Global.XBackend,
		MainShutdownFunc:   p.MainShutdownFunc,
		ServiceShutdownCtx: p.ServiceShutdownCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(p.CLIOptions.Relay.XCliOptions.HttpListenAddress, options.VERSION); err != nil {
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

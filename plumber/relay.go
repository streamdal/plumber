package plumber

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/util"
)

// HandleRelayCmd handles CLI relay mode. Container/envar mode is handled by processEnvRelayFlags
func (p *Plumber) HandleRelayCmd() error {
	if p.KongCtx == "relay" {
		// Using env vars
		p.KongCtx = "relay " + p.CLIOptions.Relay.Type
	}

	backendName, err := util.GetBackendName(p.KongCtx)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	if err := p.startRelayService(); err != nil {
		return errors.Wrap(err, "unable to start relay service")
	}

	// Blocks until ctx is cancelled
	if err := backend.Relay(p.ServiceShutdownCtx, p.RelayCh, nil); err != nil {
		return errors.Wrap(err, "unable to start relay backend")
	}

	p.log.Info("relay exiting")

	return nil
}

// startRelayService starts relay workers which send relay messages to grpc-collector
func (p *Plumber) startRelayService() error {
	relayCfg := &relay.Config{
		Token:              p.CLIOptions.Relay.Token,
		GRPCAddress:        p.CLIOptions.Relay.GRPCAddress,
		NumWorkers:         p.CLIOptions.Relay.NumWorkers,
		Timeout:            p.CLIOptions.Relay.GRPCTimeout,
		RelayCh:            p.RelayCh,
		DisableTLS:         p.CLIOptions.Relay.GRPCDisableTLS,
		BatchSize:          p.CLIOptions.Relay.BatchSize,
		Type:               p.CLIOptions.Relay.Type,
		MainShutdownFunc:   p.MainShutdownFunc,
		ServiceShutdownCtx: p.ServiceShutdownCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(p.CLIOptions.Relay.HTTPListenAddress, p.CLIOptions.Version); err != nil {
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

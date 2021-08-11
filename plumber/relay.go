package plumber

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/util"
)

// HandleRelayCmd handles CLI relay mode. Container/envar mode is handled by processEnvRelayFlags
func (p *Plumber) HandleRelayCmd() error {
	if p.Cmd == "relay" {
		// Using env vars
		p.Cmd = "relay " + p.Options.Relay.Type
	}

	backendName, err := util.GetBackendName(p.Cmd)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	if err := backend.StartRelay(context.Background(), p.RelayCh); err != nil {
		return errors.Wrap(err, "unable to start relay backend")
	}

	if err := p.startRelayService(); err != nil {
		return errors.Wrap(err, "unable to start relay service")
	}

	// Block until shutdown
	<-p.MainShutdownCtx.Done()

	p.log.Info("relay exiting")

	return nil
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

package plumber

import (
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/bus"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

// HandleRelayCmd handles CLI relay mode. Container/envar mode is handled by processEnvRelayFlags
func (p *Plumber) HandleRelayCmd() error {
	if err := validate.RelayOptionsForCLI(p.CLIOptions.Relay); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	if err := p.startRelayService(); err != nil {
		return errors.Wrap(err, "unable to start relay service")
	}

	p.Telemetry.Enqueue(posthog.Capture{
		Event:      "command_relay",
		DistinctId: p.PersistentConfig.PlumberID,
		Properties: map[string]interface{}{
			"backend": backend.Name(),
		},
	})

	// Log message prints ID on exit
	p.CLIOptions.Relay.XRelayId = "CLI"

	// Blocks until ctx is cancelled
	if err := backend.Relay(p.ServiceShutdownCtx, p.CLIOptions.Relay, p.RelayCh, nil); err != nil {
		// Shut down workers properly
		p.ServiceShutdownCtx.Done()

		return errors.Wrap(err, "unable to start relay backend")
	}

	// Block here to wait until all relay workers have shut down
	<-p.MainShutdownCtx.Done()

	p.log.Info("relay exiting")

	return nil
}

// startRelayService starts relay workers which send relay messages to grpc-collector
func (p *Plumber) startRelayService() error {
	relayCfg := &relay.Config{
		Token:              p.CLIOptions.Relay.CollectionToken,
		GRPCAddress:        p.CLIOptions.Relay.XStreamdalGrpcAddress,
		NumWorkers:         p.CLIOptions.Relay.NumWorkers,
		Timeout:            util.DurationSec(p.CLIOptions.Relay.XStreamdalGrpcTimeoutSeconds),
		RelayCh:            p.RelayCh,
		DisableTLS:         p.CLIOptions.Relay.XStreamdalGrpcDisableTls,
		BatchSize:          p.CLIOptions.Relay.BatchSize,
		Type:               p.CLIOptions.Global.XBackend,
		ServiceShutdownCtx: p.ServiceShutdownCtx,
		MainShutdownFunc:   p.MainShutdownFunc,
		MainShutdownCtx:    p.MainShutdownCtx,
		DeadLetter:         p.CLIOptions.Relay.DeadLetter,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if _, err := api.Start(&api.Config{
			PersistentConfig: p.PersistentConfig,
			Bus:              &bus.NoOpBus{},
			ListenAddress:    p.CLIOptions.Relay.XCliOptions.HttpListenAddress,
			Version:          options.VERSION,
		}); err != nil {
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

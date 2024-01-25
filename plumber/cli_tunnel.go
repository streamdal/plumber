package plumber

import (
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends"
	"github.com/streamdal/plumber/options"
	"github.com/streamdal/plumber/tunnel"
)

// HandleTunnelCmd handles tunnel destination mode commands
func (p *Plumber) HandleTunnelCmd() error {
	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	p.Telemetry.Enqueue(posthog.Capture{
		Event:      "command_tunnel",
		DistinctId: p.PersistentConfig.PlumberID,
		Properties: map[string]interface{}{
			"backend": backend.Name(),
		},
	})

	// Run up tunnel
	// Plumber cluster ID purposefully left blank here so the destination becomes ephemeral
	tunnelSvc, err := tunnel.New(p.CLIOptions.Tunnel, &tunnel.Config{
		PlumberVersion:   options.VERSION,
		PlumberClusterID: p.PersistentConfig.ClusterID,
		PlumberID:        p.PersistentConfig.PlumberID,
	})
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	// Clean up gRPC connection
	defer tunnelSvc.Close()

	errorCh := make(chan *records.ErrorRecord, 1000)

	go func() {
		for err := range errorCh {
			p.log.Errorf("Received error from tunnel component: %s", err.Error)
		}
	}()

	// Blocks until completion
	if err := backend.Tunnel(p.ServiceShutdownCtx, p.CLIOptions.Tunnel, tunnelSvc, errorCh); err != nil {
		return errors.Wrap(err, "error(s) during tunnel run")
	}

	return nil
}

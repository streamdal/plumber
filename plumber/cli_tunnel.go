package plumber

import (
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/tunnel"
)

// HandleTunnelCmd handles tunnel destination mode commands
func (p *Plumber) HandleTunnelCmd() error {
	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	// Run up tunnel
	// Plumber cluster ID purposefully left blank here so the destination becomes ephemeral
	tunnelSvc, err := tunnel.New(p.CLIOptions.Tunnel, "")
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

package plumber

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/dynamic"
)

// HandleDynamicCmd handles dynamic replay destination mode commands
func (p *Plumber) HandleDynamicCmd() error {
	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	// Run up dynamic connection
	dynamicSvc, err := dynamic.New(p.CLIOptions.Dynamic, backend.Name())
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	// Clean up gRPC connection
	defer dynamicSvc.Close()

	errorCh := make(chan *records.ErrorRecord, 1000)

	go func() {
		for err := range errorCh {
			p.log.Errorf("Received error from dynamic component: %s", err.Error)
		}
	}()

	// Blocks until completion
	if err := backend.Dynamic(p.ServiceShutdownCtx, p.CLIOptions.Dynamic, dynamicSvc, errorCh); err != nil {
		return errors.Wrap(err, "error(s) during dynamic run")
	}

	return nil
}

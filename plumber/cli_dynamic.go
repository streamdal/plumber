package plumber

import (
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

	// Start up dynamic connection
	dynamicSvc, err := dynamic.New(p.CLIOptions.Dynamic, backend.Name())
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	// Blocks until completion
	if err := backend.Dynamic(p.ServiceShutdownCtx, p.CLIOptions.Dynamic, dynamicSvc); err != nil {
		return errors.Wrap(err, "error(s) during dynamic run")
	}

	return nil
}

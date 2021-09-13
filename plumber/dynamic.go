package plumber

import (
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/backends"
)

// HandleDynamicCmd handles dynamic replay destination mode commands
func (p *Plumber) HandleDynamicCmd() error {
	backend, err := backends.New(p.CLIOptions.Global.XBackend, p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	// Blocks until completion
	if err := backend.Dynamic(p.ServiceShutdownCtx, p.CLIOptions.Dynamic); err != nil {
		return errors.Wrap(err, "error(s) during dynamic run")
	}

	return nil
}

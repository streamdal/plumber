package plumber

import (
	"context"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// HandleDynamicCmd handles dynamic replay destination mode commands
func (p *Plumber) HandleDynamicCmd() error {
	backendName, err := util.GetBackendName(p.Cmd)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	// Blocks until completion
	if err := backend.Dynamic(context.Background()); err != nil {
		return errors.Wrap(err, "error(s) during dynamic run")
	}

	return nil
}

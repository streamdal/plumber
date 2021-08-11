package plumber

import (
	"context"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// HandleWriteCmd handles write mode
func (p *Plumber) HandleWriteCmd() error {
	backendName, err := util.GetBackendName(p.Cmd)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	// TODO: What are we writing?
	if err := backend.Write(context.Background()); err != nil {
		return errors.Wrap(err, "unable to complete write(s)")
	}

	return nil
}

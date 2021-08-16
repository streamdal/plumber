package plumber

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/writer"
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

	if err := writer.ValidateWriteOptions(p.Options, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	value, err := writer.GenerateWriteMessageFromOptions(p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := backend.Write(ctx, nil, value...); err != nil {
		return errors.Wrap(err, "unable to complete write(s)")
	}

	return nil
}

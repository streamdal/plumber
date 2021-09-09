package plumber

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/writer"
)

// HandleWriteCmd handles write mode
func (p *Plumber) HandleWriteCmd() error {
	backendName, err := util.GetBackendName(p.KongCtx)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	if err := writer.ValidateWriteOptions(p.CLIOptions, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	value, err := writer.GenerateWriteMessageFromOptions(p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := backend.Write(ctx, nil, value...); err != nil {
		return errors.Wrap(err, "unable to complete write(s)")
	}

	p.log.Infof("Successfully wrote '%d' message(s) to '%s'", len(value), backendName)

	return nil
}

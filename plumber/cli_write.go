package plumber

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/validate"
	"github.com/batchcorp/plumber/writer"
)

// HandleWriteCmd handles write mode
func (p *Plumber) HandleWriteCmd() error {
	if err := validate.WriteOptionsForCLI(p.CLIOptions.Write); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to create new backend")
	}

	value, err := writer.GenerateWriteValue(p.CLIOptions.Write, p.cliFDS)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errorCh := make(chan *records.ErrorRecord, 1)

	go func() {
		if err := backend.Write(ctx, p.CLIOptions.Write, errorCh, value...); err != nil {
			p.log.Errorf("unable to complete write(s): %s", err)
		}

		cancel()
	}()

	var errRecord *records.ErrorRecord

MAIN:
	for {
		select {
		case errRecord = <-errorCh:
			err = backend.DisplayError(errRecord)
			break MAIN
		case <-ctx.Done():
			p.log.Debug("received quit from context - exiting write")
			break MAIN
		}
	}

	if errRecord == nil {
		p.log.Infof("Successfully wrote '%d' message(s)", len(value))
	}

	return nil
}

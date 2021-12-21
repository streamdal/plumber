package plumber

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/validate"
)

// HandleReadCmd handles CLI read mode
func (p *Plumber) HandleReadCmd() error {
	if err := validate.ReadOptionsForCLI(p.CLIOptions.Read); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to create new backend")
	}

	resultCh := make(chan *records.ReadRecord, 1)
	errorCh := make(chan *records.ErrorRecord, 1)

	// backend.Read() blocks
	go func() {
		if err := backend.Read(context.Background(), p.CLIOptions.Read, resultCh, errorCh); err != nil {
			p.log.Errorf("unable to complete read for backend '%s': %s", backend.Name(), err)
			os.Exit(0) // Exit out of plumber, since we can't continue
		}
	}()

MAIN:
	for {
		var err error

		select {
		case msg := <-resultCh:
			p.log.Debug("HandleReadCmd: received message on resultCh")

			decoded, decodeErr := reader.Decode(p.CLIOptions.Read, p.cliMD, msg.Payload)
			if decodeErr != nil {
				printer.Errorf("unable to decode message payload for backend '%s': %s", backend.Name(), decodeErr)

				if !p.CLIOptions.Read.Continuous {
					break MAIN
				}

				continue
			}

			msg.Payload = decoded

			err = backend.DisplayMessage(p.CLIOptions, msg)
		case errorMsg := <-errorCh:
			err = backend.DisplayError(errorMsg)
		}

		if err != nil {
			printer.Errorf("unable to display message with '%s' backend: %s", backend.Name(), err)
		}

		if !p.CLIOptions.Read.Continuous {
			break MAIN
		}
	}

	return nil
}

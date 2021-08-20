package plumber

import (
	"context"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
	"github.com/pkg/errors"
)

// HandleReadCmd handles CLI read mode
func (p *Plumber) HandleReadCmd() error {
	backendName, err := util.GetBackendName(p.Cmd)
	if err != nil {
		return errors.Wrap(err, "unable to get backend name")
	}

	if err := validate.BaseReadOptions(p.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	backend, err := backends.New(backendName, p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new backend")
	}

	resultCh := make(chan *types.ReadMessage, 1)
	errorCh := make(chan *types.ErrorMessage, 1)

	// .Read() might be a blocking
	go func() {
		if err := backend.Read(context.Background(), resultCh, errorCh); err != nil {
			p.log.Errorf("unable to complete read for backend '%s': %s", backend.Name(), err)
		}
	}()

MAIN:
	for {
		var err error

		select {
		case msg := <-resultCh:
			p.log.Debug("HandleReadCmd: received message on resultCh")

			err = backend.DisplayMessage(msg)
		case errorMsg := <-errorCh:
			err = backend.DisplayError(errorMsg)
		}

		if err != nil {
			printer.Errorf("unable to display message with '%s' backend: %s", backend.Name(), err)
		}

		if !p.Options.Read.Follow {
			break MAIN
		}
	}

	return nil
}

package plumber

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/backends"
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

	if err := validate.ReadOptions(p.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	backend, err := backends.New(backendName, p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new backend")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := backend.Connect(ctx); err != nil {
		return errors.Wrap(err, "unable to connect backend")
	}

	resultCh := make(chan []*types.Message, 1000)
	errorCh := make(chan []*types.ErrorMessage, 100)

	// Non-blocking call
	if err := backend.Read(ctx, resultCh, errorCh); err != nil {
		return errors.Wrap(err, "unable to read data")
	}

MAIN:
	for {
		select {
		case messages := <-resultCh:
			p.displayRead(messages)
		case messages := <-errorCh:
			p.displayErrors(messages)
		case <-ctx.Done():
			p.log.Debug("cancelled via context")
			break MAIN
		}
	}

	return nil
}

// TODO: Implement
func (p *Plumber) displayRead(messages []*types.Message) {

}

// TODO: implement
func (p *Plumber) displayErrors(messages []*types.ErrorMessage) {

}

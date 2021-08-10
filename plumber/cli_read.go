package plumber

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// handleReadCmd handles CLI read mode
func (p *Plumber) handleReadCmd() error {
	backendName, err := util.GetBackendName(p.Cmd)
	if err != nil {
		return errors.Wrap(err, "unable to get backend name")
	}

	if err := validateReadOptions(p.Options); err != nil {
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

	// Non-blocking call
	if err := backend.Read(ctx, resultCh); err != nil {
		return errors.Wrap(err, "unable to read data")
	}

MAIN:
	for {
		select {
		case messages := <-resultCh:
			p.displayRead(messages)
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

func validateReadOptions(opts *options.Options) error {
	if opts.Read == nil {
		return errors.New("read options cannot be nil")
	}

	if opts.Read.OutputChannel == nil {
		return errors.New("output channel cannot be nil")
	}

	return nil
}

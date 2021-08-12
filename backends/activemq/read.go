package activemq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Read(ctx context.Context, resultsCh chan *types.ReadMessage, errorCh chan *types.ErrorMessage) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	if err := validateReadOptions(a.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	if err := a.read(ctx, resultsCh, errorCh); err != nil {
		return errors.Wrap(err, "error(s) performing read")
	}

	return nil
}

func (a *ActiveMq) read(ctx context.Context, resultsCh chan *types.ReadMessage, errorCh chan *types.ErrorMessage) error {
	a.log.Info("Listening for message(s) ...")

	count := 1

	sub, err := a.client.Subscribe(a.getDestination(), stomp.AckClient)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

MAIN:
	for {
		select {
		case msg := <-sub.C:
			if err := a.handleMessage(msg, resultsCh, count); err != nil {
				wrappedError := errors.Wrap(err, "unable to handle message")
				util.WriteError(a.log, errorCh, wrappedError)
			}
		case <-ctx.Done():
			a.log.Debug("read cancelled via context")
			break MAIN
		}

		if !a.Options.Read.Follow {
			return nil
		}

		count++
	}

	a.log.Debug("reader exiting")

	return nil
}

func (a *ActiveMq) handleMessage(msg *stomp.Message, resultsCh chan *types.ReadMessage, count int) error {
	data, err := reader.Decode(a.Options, msg.Body)
	if err != nil {
		return errors.Wrap(err, "unable to decode message")
	}

	resultsCh <- &types.ReadMessage{
		ReceivedAt: time.Now().UTC(),
		Num:        count,
		Value:      data,
	}

	a.client.Ack(msg)

	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.ActiveMq.Topic != "" && opts.ActiveMq.Queue != "" {
		return errors.New("you may only specify a \"topic\" or a \"queue\" not both")
	}

	return nil
}

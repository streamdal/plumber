package activemq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Read(ctx context.Context, resultsCh chan *types.Message) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	if err := validateReadOptions(a.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	if err := a.read(ctx, resultsCh); err != nil {
		return errors.Wrap(err, "error(s) performing read")
	}

	return nil
}

func (a *ActiveMq) read(ctx context.Context, resultsCh chan *types.Message) error {
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
				a.log.Errorf("unable to handle message (count %d): %s", count, err)
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

func (a *ActiveMq) handleMessage(msg *stomp.Message, resultsCh chan *types.Message, count int) error {
	data, err := reader.Decode(a.Options, msg.Body)
	if err != nil {
		return errors.Wrap(err, "unable to decode message")
	}

	resultsCh <- &types.Message{
		ReceivedAt: time.Now().UTC(),
		MessageNum: count,
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

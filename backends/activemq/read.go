package activemq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Read(ctx context.Context, resultsCh chan *types.ReadMessage, errorCh chan *types.ErrorMessage) error {
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
			resultsCh <- &types.ReadMessage{
				ReceivedAt: time.Now().UTC(),
				Num:        count,
				Value:      msg.Body,
				Raw:        msg,
			}

			if err := a.client.Ack(msg); err != nil {
				util.WriteError(a.log, errorCh, errors.Wrap(err, "unable to ack message"))
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

func validateReadOptions(opts *options.Options) error {
	if opts.ActiveMq.Topic != "" && opts.ActiveMq.Queue != "" {
		return errors.New("you may only specify a \"topic\" or a \"queue\" not both")
	}

	return nil
}

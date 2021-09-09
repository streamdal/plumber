package activemq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp/v3"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Read(ctx context.Context, resultsCh chan *types.ReadMessage, errorCh chan *types.ErrorMessage) error {
	if err := validateReadOptions(a.ConnectionConfig); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := newConn(ctx, a.ConnectionConfig)
	if err != nil {
		return errors.Wrap(err, "unable to establish new connection")
	}

	if err := a.read(ctx, client, resultsCh, errorCh); err != nil {
		return errors.Wrap(err, "error(s) performing read")
	}

	return nil
}

func (a *ActiveMq) read(ctx context.Context, conn *stomp.Conn, resultsCh chan *types.ReadMessage, errorCh chan *types.ErrorMessage) error {
	a.log.Info("Listening for message(s) ...")

	count := 1

	sub, err := conn.Subscribe(a.getDestination(), stomp.AckAuto)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

MAIN:
	for {
		select {
		case msg := <-sub.C:
			// Avoid a potential panic when we're unable to ack
			if msg == nil {
				continue MAIN
			}

			resultsCh <- &types.ReadMessage{
				ReceivedAt: time.Now().UTC(),
				Num:        count,
				Value:      msg.Body,
				Raw:        msg,
			}
		case <-ctx.Done():
			a.log.Debug("read cancelled via context")
			break MAIN
		}

		if !a.ConnectionConfig.Read.Follow {
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

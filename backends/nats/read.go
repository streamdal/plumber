package nats

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

func (n *Nats) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	n.log.Info("Listening for message(s) ...")

	count := 1

	// nats.Subscribe is async, use channel to wait to exit
	doneCh := make(chan struct{})
	defer close(doneCh)

	if _, err := n.client.Subscribe(n.Options.Nats.Subject, func(msg *nats.Msg) {
		resultsChan <- &types.ReadMessage{
			Value: msg.Data,
			Metadata: map[string]interface{}{
				"subject": msg.Subject,
				"reply":   msg.Reply,
			},
			Raw: msg,
		}

		count++

		if !n.Options.Read.Follow {
			doneCh <- struct{}{}
		}
	}); err != nil {
		util.WriteError(n.log, errorChan, errors.Wrap(err, "error during subscribe"))
		return errors.Wrap(err, "error during subscribe")
	}

	<-doneCh

	return nil
}

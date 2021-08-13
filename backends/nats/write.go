package nats

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// Write performs necessary setup and calls Nats.Write() to write the actual message
func (n *Nats) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	for _, msg := range messages {
		if err := n.write(msg.Value); err != nil {
			util.WriteError(n.log, errorCh, err)
		}
	}

	return nil
}

// Write publishes a message to a NATS subject
func (n *Nats) write(value []byte) error {
	if err := n.client.Publish(n.Options.Nats.Subject, value); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	n.log.Infof("Successfully wrote message to '%s'", n.Options.Nats.Subject)
	return nil
}

package nats_streaming

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
)

func (n *NatsStreaming) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	for _, msg := range messages {
		if err := n.write(msg.Value); err != nil {
			util.WriteError(n.log, errorCh, err)
		}
	}

	return nil
}

// Write publishes a message to a NATS streaming channel. The publish is synchronous, and will not complete until
// an ACK has been received by the server
func (n *NatsStreaming) write(value []byte) error {
	if err := n.stanClient.Publish(n.Options.NatsStreaming.Channel, value); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	n.log.Infof("Successfully wrote message to channel '%s'", n.Options.NatsStreaming.Channel)
	return nil
}

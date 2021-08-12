package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// Write is the entry point function for performing write operations in GCP PubSub.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (g *GCPPubSub) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	for _, msg := range messages {
		if err := g.write(ctx, msg.Value); err != nil {
			util.WriteError(g.log, errorCh, err)
		}
	}

	g.log.Debug("finished writing messages")

	return nil
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (g *GCPPubSub) write(ctx context.Context, value []byte) error {
	t := g.client.Topic(g.Options.GCPPubSub.WriteTopicId)

	result := t.Publish(ctx, &pubsub.Message{
		Data: value,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	_, err := result.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to ensure that message was published")
	}

	return nil
}

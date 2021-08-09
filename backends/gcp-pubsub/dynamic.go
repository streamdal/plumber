package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	ctx := context.Background()
	llog := logrus.WithField("pkg", "gcppubsub/dynamic")

	// Start up writer
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	defer client.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "GCP PubSub")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	t := client.Topic(opts.GCPPubSub.WriteTopicId)

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:

			result := t.Publish(ctx, &pubsub.Message{
				Data: outbound.Blob,
			})

			// Block until the result is returned and a server-generated
			// ID is returned for the published message.
			_, err := result.Get(ctx)
			if err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to GCP Pubsub topic '%s' for replay '%s'", opts.GCPPubSub.WriteTopicId, outbound.ReplayId)
		}
	}
}

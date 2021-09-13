package rpubsub

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dynamic"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (r *Redis) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "rpubsub/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(r.Options, "Redis PubSub")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			for _, ch := range r.Options.RedisPubSub.Channels {
				err := r.client.Publish(context.Background(), ch, outbound.Blob).Err()
				if err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}

				llog.Debugf("Replayed message to Redis PubSub channel '%s' for replay '%s'", ch, outbound.ReplayId)
			}
		case <-ctx.Done():
			llog.Debug("context closed")
			break MAIN
		}
	}

	llog.Debug("exiting")

	return nil
}

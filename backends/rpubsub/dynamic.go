package rpubsub

import (
	"context"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *cli.Options) error {
	llog := logrus.WithField("pkg", "rpubsub/dynamic")

	// Start up writer
	writer, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to connect to Redis PubSub")
	}

	defer writer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "Redis PubSub")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			for _, ch := range opts.RedisPubSub.Channels {
				err := writer.Publish(context.Background(), ch, outbound.Blob).Err()
				if err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}

				llog.Debugf("Replayed message to Redis PubSub channel '%s' for replay '%s'", ch, outbound.ReplayId)
			}
		}
	}
}

package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	llog := logrus.WithField("pkg", "rstreams/dynamic")

	// Start up writer
	writer, err := NewStreamsClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to connect to Redis Streams")
	}

	defer writer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "Redis Streams")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			for _, streamName := range opts.RedisStreams.Streams {
				_, err := writer.XAdd(context.Background(), &redis.XAddArgs{
					Stream: streamName,
					ID:     opts.RedisStreams.WriteID,
					Values: map[string]interface{}{
						opts.RedisStreams.WriteKey: outbound.Blob,
					},
				}).Result()
				if err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}

				llog.Debugf("Replayed message to Redis stream '%s' for replay '%s'", streamName, outbound.ReplayId)
			}
		}
	}
}

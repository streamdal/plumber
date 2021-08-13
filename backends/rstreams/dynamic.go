package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (r *RedisStreams) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "rstreams/dynamic")

	// Start up dynamic connection
	grpc, err := dproxy.New(r.Options, "Redis Streams")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			for _, streamName := range r.Options.RedisStreams.Streams {
				_, err := r.client.XAdd(context.Background(), &redis.XAddArgs{
					Stream: streamName,
					ID:     r.Options.RedisStreams.WriteID,
					Values: map[string]interface{}{
						r.Options.RedisStreams.WriteKey: outbound.Blob,
					},
				}).Result()
				if err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}

				llog.Debugf("Replayed message to Redis stream '%s' for replay '%s'", streamName, outbound.ReplayId)
			}
		case <-ctx.Done():
			r.log.Debug("context cancelled")
			break MAIN
		}
	}

	r.log.Debug("dynamic exiting")

	return nil
}

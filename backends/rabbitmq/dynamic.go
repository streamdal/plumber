package rabbitmq

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (r *RabbitMQ) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "rabbitmq/dynamic")

	client, err := newConnection(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new rabbit client")
	}

	defer client.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(r.Options, "RabbitMQ")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := client.Publish(ctx, r.Options.Rabbit.RoutingKey, outbound.Blob); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to rabbitmq routing key '%s' for replay '%s'",
				r.Options.Rabbit.RoutingKey, outbound.ReplayId)

		case <-ctx.Done():
			llog.Debug("context cancelled")
			break MAIN
		}
	}

	llog.Debug("exiting")

	return nil
}

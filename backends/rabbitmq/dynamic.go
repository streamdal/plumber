package rabbitmq

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	ctx := context.Background()
	llog := logrus.WithField("pkg", "rabbitmq/dynamic")

	// Start up writer
	writer, err := New(opts, nil)
	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq publisher")
	}

	defer writer.Consumer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "RabbitMQ")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := writer.Consumer.Publish(ctx, opts.Rabbit.RoutingKey, outbound.Blob); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to rabbitmq routing key '%s' for replay '%s'", opts.Rabbit.RoutingKey, outbound.ReplayId)
		}
	}
}

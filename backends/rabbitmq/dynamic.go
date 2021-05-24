package rabbitmq

import (
	"context"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func Dynamic(opts *cli.Options) error {
	ctx := context.Background()
	log := logrus.WithField("pkg", "rabbitmq/dynamic")

	// Start up writer
	writer, err := New(opts, nil)
	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq publisher")
	}

	defer writer.Consumer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "rabbitmq")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := writer.Consumer.Publish(ctx, opts.Rabbit.RoutingKey, outbound.Blob); err != nil {
				return errors.Wrap(err, "unable to replay message to rabbitmq")
			}

			log.Debugf("Replayed message to rabbitmq routing key '%s' for replay '%s'", opts.Rabbit.RoutingKey, outbound.ReplayId)
		}
	}
}

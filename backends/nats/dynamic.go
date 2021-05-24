package nats

import (
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *cli.Options) error {
	log := logrus.WithField("pkg", "nats/dynamic")

	// Start up client
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq publisher")
	}

	defer client.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "nats")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := client.Publish(opts.Nats.Subject, outbound.Blob); err != nil {
				return errors.Wrap(err, "unable to replay message to Nats")
			}

			log.Debugf("Replayed message to Nats topic '%s' for replay '%s'", opts.Nats.Subject, outbound.ReplayId)
		}
	}
}

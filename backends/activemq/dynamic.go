package activemq

import (
	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	llog := logrus.WithField("pkg", "activemq/dynamic")

	// Start up writer
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to connect to ActiveMQ")
	}

	defer client.Disconnect()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "ActiveMQ")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	destination := getDestination(opts)

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := client.Send(destination, "", outbound.Blob, nil); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to ActiveMQ '%s' for replay '%s'", destination, outbound.ReplayId)
		}
	}
}

func getDestination(opts *options.Options) string {
	if opts.ActiveMq.Topic != "" {
		return "/topic/" + opts.ActiveMq.Topic
	}
	return opts.ActiveMq.Queue
}

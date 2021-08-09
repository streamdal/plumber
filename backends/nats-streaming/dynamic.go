package nats_streaming

import (
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	llog := logrus.WithField("pkg", "nats-streaming/dynamic")

	// Start up writer
	writer, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to initialize nats streaming publisher")
	}

	defer writer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "Nats Streaming")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	sub, err := stan.Connect(opts.NatsStreaming.ClusterID, opts.NatsStreaming.ClientID, stan.NatsConn(writer))
	if err != nil {
		return errors.Wrap(err, "could not create NATS subscription")
	}

	defer sub.Close()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := sub.Publish(opts.NatsStreaming.Channel, outbound.Blob); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to NATS streaming channel '%s' for replay '%s'", opts.NatsStreaming.Channel, outbound.ReplayId)
		}
	}
}

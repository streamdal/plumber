package nats_streaming

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/writer"
)

func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	natsClient, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	defer natsClient.Close()

	stanClient, err := stan.Connect(opts.NatsStreaming.ClusterID, opts.NatsStreaming.ClientID, stan.NatsConn(natsClient))
	if err != nil {
		return errors.Wrap(err, "could not create NATS subscription")
	}

	defer stanClient.Close()

	n := &NatsStreaming{
		Options:    opts,
		MsgDesc:    md,
		Client:     natsClient,
		StanClient: stanClient,
		log:        logrus.WithField("pkg", "nats-streaming/write.go"),
		printer:    printer.New(),
	}

	for _, value := range writeValues {
		if err := n.Write(value); err != nil {
			n.log.Error(err)
		}
	}

	return nil
}

// Write publishes a message to a NATS streaming channel. The publish is synchronous, and will not complete until
// an ACK has been received by the server
func (n *NatsStreaming) Write(value []byte) error {
	if err := n.StanClient.Publish(n.Options.NatsStreaming.Channel, value); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	n.log.Infof("Successfully wrote message to channel '%s'", n.Options.NatsStreaming.Channel)
	return nil
}

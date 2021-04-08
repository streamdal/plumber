package nats_streaming

import (
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/writer"
	"github.com/jhump/protoreflect/desc"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.WriteInputType == "jsonpb" {
		md, mdErr = pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	value, err := writer.GenerateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	nc, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	n := &NatsStreaming{
		Options: opts,
		MsgDesc: md,
		Client:  nc,
		log:     logrus.WithField("pkg", "nats-streaming/write.go"),
		printer: printer.New(),
	}

	return n.Write(value)
}

// Write publishes a message to a NATS streaming channel. The publish is synchronous, and will not complete until
// an ACK has been received by the server
func (n *NatsStreaming) Write(value []byte) error {
	defer n.Client.Close()

	sub, err := stan.Connect(n.Options.NatsStreaming.ClusterID, n.Options.NatsStreaming.ClientID, stan.NatsConn(n.Client))
	if err != nil {
		return errors.Wrap(err, "could not create NATS subscription")
	}

	defer sub.Close()

	if err := sub.Publish(n.Options.NatsStreaming.Channel, value); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	n.log.Infof("Successfully wrote message to channel '%s'", n.Options.NatsStreaming.Channel)
	return nil
}

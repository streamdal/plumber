package nats

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/writer"
)

// Write performs necessary setup and calls Nats.Write() to write the actual message
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

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	n := &Nats{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "nats/write.go"),
	}

	return n.Write(value)
}

// Write publishes a message to a NATS subject
func (n *Nats) Write(value []byte) error {
	defer n.Client.Close()
	if err := n.Client.Publish(n.Options.Nats.Subject, value); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	n.log.Infof("Successfully wrote message to '%s'", n.Options.Nats.Subject)
	return nil
}

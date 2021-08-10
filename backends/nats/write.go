package nats

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Write performs necessary setup and calls Nats.Write() to write the actual message
func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	defer client.Close()

	n := &Nats{
		Options: opts,
		msgDesc: md,
		client:  client,
		log:     logrus.WithField("pkg", "nats/write.go"),
	}

	for _, value := range writeValues {
		if err := n.Write(value); err != nil {
			n.log.Error(err)
		}
	}

	return nil
}

// Write publishes a message to a NATS subject
func (n *Nats) Write(value []byte) error {
	if err := n.client.Publish(n.Options.Nats.Subject, value); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	n.log.Infof("Successfully wrote message to '%s'", n.Options.Nats.Subject)
	return nil
}

package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"
)

func Write(opts *cli.Options, md *desc.MessageDescriptor) error {
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

	p := &Pulsar{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "pulsar/write.go"),
	}

	for _, value := range writeValues {
		if err := p.Write(value); err != nil {
			p.log.Error(err)
		}
	}

	return nil
}

// Write writes a message to an ActiveMQ topic
func (p *Pulsar) Write(value []byte) error {

	producer, err := p.Client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.Options.Pulsar.Topic,
	})

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: value,
	})

	defer producer.Close()

	if err != nil {
		p.log.Infof("Unable to write message to topic '%s': %s", p.Options.Pulsar.Topic, err)
		return errors.Wrap(err, "unable to write message")
	}

	p.log.Infof("Successfully wrote message to topic '%s'", p.Options.Pulsar.Topic)

	return nil
}

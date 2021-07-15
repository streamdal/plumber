package rabbitmqStreams

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"
)

func Write(opts *cli.Options, md *desc.MessageDescriptor) error {

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return err
	}

	defer client.Close()

	producer, err := client.NewProducer(opts.RabbitMQStreams.Stream, &stream.ProducerOptions{
		Name:      opts.RabbitMQStreams.ClientName,
		QueueSize: 0,
		BatchSize: len(writeValues),
	})
	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq streams producer")
	}

	defer producer.Close()

	r := &RabbitMQStreams{
		Client:   client,
		Producer: producer,
		Options:  opts,
		log:      logrus.WithField("pkg", "rabbitmq-streams/write.go"),
	}

	for _, value := range writeValues {
		if err := r.Write(value); err != nil {
			r.log.Error(err)
			continue
		}
	}

	return nil
}

func (r *RabbitMQStreams) Write(value []byte) error {
	if err := r.Producer.Send(amqp.NewMessage(value)); err != nil {
		return errors.Wrapf(err, "unable to publish message to stream '%s'", r.Options.RabbitMQStreams.Stream)
	}

	r.log.Infof("Published message to stream '%s'", r.Options.RabbitMQStreams.Stream)

	return nil
}

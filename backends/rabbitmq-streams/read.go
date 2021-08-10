package rabbitmq_streams

import (
	"fmt"
	"strconv"

	"github.com/batchcorp/plumber/printer"

	"github.com/batchcorp/plumber/reader"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	client, err := NewClient(opts)
	if err != nil {
		return err
	}

	r := &RabbitMQStreams{
		client:  client,
		Options: opts,
		msgDesc: md,
		log:     logrus.WithField("pkg", "rabbitmq-streams/read.go"),
	}

	return r.Read()
}

func (r *RabbitMQStreams) Read() error {
	var count int

	offsetOption, err := r.getOffsetOption()
	if err != nil {
		return errors.Wrap(err, "could not read messages")
	}

	handleMessage := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		for _, value := range message.Data {
			count++
			data, err := reader.Decode(r.Options, r.msgDesc, value)
			if err != nil {
				r.log.Error(err)
				continue
			}

			printer.PrintRabbitMQStreamsResult(r.Options, count, consumerContext, message, data)
		}

		if !r.Options.Read.Follow {
			consumerContext.Consumer.Close()
		}
	}

	consumer, err := r.client.NewConsumer(r.Options.RabbitMQStreams.Stream,
		handleMessage,
		stream.NewConsumerOptions().
			SetConsumerName(r.Options.RabbitMQStreams.ClientName).
			SetOffset(offsetOption))
	if err != nil {
		return errors.Wrap(err, "unable to start rabbitmq streams consumer")
	}

	r.log.Infof("Waiting for messages on stream '%s'...", r.Options.RabbitMQStreams.Stream)

	closeCh := consumer.NotifyClose()

	select {
	case closeEvent := <-closeCh:
		// TODO: implement reconnect logic
		r.log.Debugf("Stream closed by remote host: %s", closeEvent.Reason)
	}

	return nil
}

func (r *RabbitMQStreams) getOffsetOption() (stream.OffsetSpecification, error) {
	offset := r.Options.RabbitMQStreams.Offset

	switch offset {
	case "last":
		return stream.OffsetSpecification{}.Last(), nil
	case "last-consumed":
		return stream.OffsetSpecification{}.LastConsumed(), nil
	case "first":
		return stream.OffsetSpecification{}.First(), nil
	case "next":
		return stream.OffsetSpecification{}.Next(), nil
	default:
		if v, err := strconv.ParseInt(offset, 10, 64); err == nil {
			return stream.OffsetSpecification{}.Offset(v), nil
		} else {
			return stream.OffsetSpecification{}.Next(),
				fmt.Errorf("unknown --offset value '%s'", offset)
		}
	}
}

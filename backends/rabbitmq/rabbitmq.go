package rabbitmq

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/cli"
)

// Reader holds all attributes required for performing a read/write operations
// in RabbitMQ. This struct should be instantiated via the rabbitmq.Read(..) or
// rabbitmq.Write(..) functions.
type RabbitMQ struct {
	Options *cli.Options
	Channel *amqp.Channel
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

func connect(opts *cli.Options) (*amqp.Channel, error) {
	ac, err := amqp.Dial(opts.Rabbit.Address)
	if err != nil {
		return nil, err
	}

	ch, err := ac.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if opts.Action == "read" {
		if _, err = ch.QueueDeclare(
			opts.Rabbit.ReadQueue,
			opts.Rabbit.ReadQueueDurable,
			opts.Rabbit.ReadQueueAutoDelete,
			opts.Rabbit.ReadQueueExclusive,
			false,
			nil,
		); err != nil {
			return nil, errors.Wrap(err, "unable to declare queue")
		}

		// Do not bind if using default exchange
		if opts.Rabbit.Exchange != "" {
			if err := ch.QueueBind(
				opts.Rabbit.ReadQueue,
				opts.Rabbit.RoutingKey,
				opts.Rabbit.Exchange,
				false,
				nil,
			); err != nil {
				return nil, errors.Wrap(err, "unable to bind queue")
			}
		}
	}

	return ch, nil
}

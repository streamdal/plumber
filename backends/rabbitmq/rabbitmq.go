package rabbitmq

import (
	"fmt"
	"os"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/urfave/cli/v2"
)

type RabbitMQ struct {
	Options *Options
	Channel *amqp.Channel
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

type Options struct {
	Action              string
	Address             string
	ExchangeName        string
	RoutingKey          string
	QueueName           string
	QueueDurable        bool
	QueueAutoDelete     bool
	QueueExclusive      bool
	Follow              bool
	LineNumbers         bool
	OutputType          string
	Convert             string
	ProtobufDir         string
	ProtobufRootMessage string
	InputFile           string
	InputData           string
	InputType           string
}

func parseOptions(c *cli.Context) (*Options, error) {
	if len(os.Args) < 1 {
		return nil, fmt.Errorf("unexpected number of args (%d)", len(os.Args))
	}

	if strings.HasPrefix(os.Args[1], "-") {
		return nil, errors.New("first arg cannot be a flag")
	}

	return &Options{
		Action:              os.Args[1],
		Address:             c.String("address"),
		ExchangeName:        c.String("exchange"),
		RoutingKey:          c.String("routing-key"),
		QueueName:           c.String("queue"),
		QueueDurable:        c.Bool("queue-durable"),
		QueueAutoDelete:     c.Bool("queue-auto-delete"),
		QueueExclusive:      c.Bool("queue-exclusive"),
		LineNumbers:         c.Bool("line-numbers"),
		Follow:              c.Bool("follow"),
		OutputType:          c.String("output-type"),
		Convert:             c.String("convert"),
		ProtobufDir:         c.String("protobuf-dir"),
		ProtobufRootMessage: c.String("protobuf-root-message"),
		InputData:           c.String("input-data"),
		InputFile:           c.String("input-file"),
		InputType:           c.String("input-type"),
	}, nil
}

func connect(opts *Options) (*amqp.Channel, error) {
	ac, err := amqp.Dial(opts.Address)
	if err != nil {
		return nil, err
	}

	ch, err := ac.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if opts.Action == "read" {
		if _, err = ch.QueueDeclare(
			opts.QueueName,
			opts.QueueDurable,
			opts.QueueAutoDelete,
			opts.QueueExclusive,
			false,
			nil,
		); err != nil {
			return nil, errors.Wrap(err, "unable to declare queue")
		}

		// Do not bind if using default exchange
		if opts.ExchangeName != "" {
			if err := ch.QueueBind(
				opts.QueueName,
				opts.RoutingKey,
				opts.ExchangeName,
				false,
				nil,
			); err != nil {
				return nil, errors.Wrap(err, "unable to bind queue")
			}
		}
	}

	return ch, nil
}

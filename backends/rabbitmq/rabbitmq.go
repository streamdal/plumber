package rabbitmq

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/rabbit"
)

const (
	BackendName = "rabbitmq"
)

// RabbitMQ holds all attributes required for performing a read/write operations
// in RabbitMQ. This struct should be instantiated via the rabbitmq.Read(..) or
// rabbitmq.Write(..) functions.
type RabbitMQ struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.RabbitConn

	log *logrus.Entry
}

func New(opts *opts.ConnectionOptions) (*RabbitMQ, error) {
	if err := validateBaseConnOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	connArgs := opts.GetRabbit()

	r := &RabbitMQ{
		connOpts: opts,
		connArgs: connArgs,
		log:      logrus.WithField("backend", "rabbitmq"),
	}

	return r, nil
}

func (r *RabbitMQ) newRabbitForRead(readArgs *args.RabbitReadArgs) (*rabbit.Rabbit, error) {
	rmq, err := rabbit.New(&rabbit.Options{
		URL:            r.connArgs.Address,
		QueueName:      readArgs.QueueName,
		ExchangeName:   readArgs.ExchangeName,
		RoutingKey:     readArgs.BindingKey,
		QueueExclusive: readArgs.QueueExclusive,
		QueueDurable:   readArgs.QueueDurable,
		QueueDeclare:   readArgs.QueueDeclare,
		AutoAck:        readArgs.AutoAck,
		ConsumerTag:    readArgs.ConsumerTag,
		UseTLS:         r.connArgs.UseTls,
		SkipVerifyTLS:  r.connArgs.InsecureTls,
		Mode:           rabbit.Consumer,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	return rmq, nil
}

func (r *RabbitMQ) newRabbitForWrite(writeArgs *args.RabbitWriteArgs) (*rabbit.Rabbit, error) {
	rmq, err := rabbit.New(&rabbit.Options{
		URL:                r.connArgs.Address,
		ExchangeName:       writeArgs.ExchangeName,
		RoutingKey:         writeArgs.RoutingKey,
		ExchangeDeclare:    writeArgs.ExchangeDeclare,
		ExchangeDurable:    writeArgs.ExchangeDurable,
		ExchangeAutoDelete: writeArgs.ExchangeAutoDelete,
		ExchangeType:       writeArgs.ExchangeType,
		AppID:              writeArgs.AppId,
		UseTLS:             r.connArgs.UseTls,
		SkipVerifyTLS:      r.connArgs.InsecureTls,
		Mode:               rabbit.Producer,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	return rmq, nil
}

func (r *RabbitMQ) Name() string {
	return BackendName
}

func (r *RabbitMQ) Close(_ context.Context) error {
	return nil
}

func (r *RabbitMQ) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return errors.New("connection config cannot be nil")
	}

	if connOpts.Conn == nil {
		return errors.New("connection object in connection config cannot be nil")
	}

	if connOpts.GetRabbit() == nil {
		return errors.New("connection config args cannot be nil")
	}

	return nil
}

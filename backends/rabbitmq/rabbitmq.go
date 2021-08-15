package rabbitmq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/rabbit"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// RabbitMQ holds all attributes required for performing a read/write operations
// in RabbitMQ. This struct should be instantiated via the rabbitmq.Read(..) or
// rabbitmq.Write(..) functions.
type RabbitMQ struct {
	Options *options.Options

	log *logrus.Entry
}

func New(opts *options.Options) (*RabbitMQ, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &RabbitMQ{
		Options: opts,
		log:     logrus.WithField("backend", "rabbitmq"),
	}, nil
}

func (r *RabbitMQ) Close(ctx context.Context) error {
	return nil
}

func (r *RabbitMQ) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (r *RabbitMQ) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func newConnection(opts *options.Options) (*rabbit.Rabbit, error) {
	mode := rabbit.Consumer

	if opts.Action == "write" {
		mode = rabbit.Producer
	}

	rmq, err := rabbit.New(&rabbit.Options{
		URL:            opts.Rabbit.Address,
		QueueName:      opts.Rabbit.ReadQueue,
		ExchangeName:   opts.Rabbit.Exchange,
		RoutingKey:     opts.Rabbit.RoutingKey,
		QueueExclusive: opts.Rabbit.ReadQueueExclusive,
		QueueDurable:   opts.Rabbit.ReadQueueDurable,
		QueueDeclare:   opts.Rabbit.ReadQueueDeclare,
		AutoAck:        opts.Rabbit.ReadAutoAck,
		ConsumerTag:    opts.Rabbit.ReadConsumerTag,
		AppID:          opts.Rabbit.WriteAppID,
		UseTLS:         opts.Rabbit.UseTLS,
		SkipVerifyTLS:  opts.Rabbit.SkipVerifyTLS,
		Mode:           mode,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize rabbitmq client")
	}

	return rmq, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.Rabbit == nil {
		return errors.New("rabbit options cannot be nil")
	}

	return nil
}

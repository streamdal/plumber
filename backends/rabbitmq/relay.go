package rabbitmq

import (
	"context"

	"github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/plumber/stats"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/rabbit"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
)

type Relayer struct {
	Options         *cli.Options
	Channel         *amqp.Channel
	RelayCh         chan interface{}
	log             *logrus.Entry
	Looper          *director.FreeLooper
	ShutdownContext context.Context
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	return &Relayer{
		Options:         opts,
		RelayCh:         relayCh,
		log:             logrus.WithField("pkg", "rabbitmq/relay"),
		Looper:          director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownContext: shutdownCtx,
	}, nil
}

func validateRelayOptions(opts *cli.Options) error {
	if opts.Rabbit.RoutingKey == "" {
		return errors.New("You must specify a routing key")
	}
	if opts.Rabbit.ReadQueue == "" {
		return errors.New("You must specify a queue to read from")
	}
	if opts.Rabbit.Exchange == "" {
		return errors.New("You must specify an exchange")
	}
	return nil
}

func (r *Relayer) Relay() error {

	errCh := make(chan *rabbit.ConsumeError)

	r.log.Infof("Relaying RabbitMQ messages from '%s' exchange -> '%s'",
		r.Options.Rabbit.Exchange, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	rmq, err := rabbit.New(&rabbit.Options{
		URL:           r.Options.Rabbit.Address,
		QueueName:     r.Options.Rabbit.ReadQueue,
		ExchangeName:  r.Options.Rabbit.Exchange,
		RoutingKey:    r.Options.Rabbit.RoutingKey,
		AutoAck:       r.Options.Rabbit.ReadAutoAck,
		QueueDeclare:  r.Options.Rabbit.ReadQueueDeclare,
		QueueDurable:  r.Options.Rabbit.ReadQueueDurable,
		ConsumerTag:   r.Options.Rabbit.ReadConsumerTag,
		UseTLS:        r.Options.Rabbit.UseTLS,
		SkipVerifyTLS: r.Options.Rabbit.SkipVerifyTLS,
	})
	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	defer rmq.Close()

	go rmq.Consume(r.ShutdownContext, errCh, func(msg amqp.Delivery) error {
		if msg.Body == nil {
			// Ignore empty messages
			// this will also prevent log spam if a queue goes missing
			return nil
		}

		stats.Incr("rabbit-relay-consumer", 1)

		r.log.Debugf("Writing RabbitMQ message to relay channel: %+v", msg)

		r.RelayCh <- &types.RelayMessage{
			Value:   &msg,
			Options: &types.RelayMessageOptions{},
		}
		return nil
	})

	for {
		select {
		case errRabbit := <-errCh:
			r.log.Errorf("runFunc ran into an error: %s", errRabbit.Error.Error())
		case <-r.ShutdownContext.Done():
			r.log.Info("Received shutdown signal, existing relayer")
			return nil
		default:
			// noop
		}
	}

	return nil
}

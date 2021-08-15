package rabbitmq

import (
	"context"

	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	rtypes "github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/rabbit"
)

type Relayer struct {
	Options     *options.Options
	Channel     *amqp.Channel
	RelayCh     chan interface{}
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
}

func (r *RabbitMQ) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	if err := validateRelayOptions(r.Options); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	client, err := newConnection(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new rabbit client")
	}

	defer client.Close()

	errCh := make(chan *rabbit.ConsumeError)

	r.log.Infof("Relaying RabbitMQ messages from '%s' exchange -> '%s'",
		r.Options.Rabbit.Exchange, r.Options.Relay.GRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.Relay.HTTPListenAddress)

	go client.Consume(ctx, errCh, func(msg amqp.Delivery) error {
		if msg.Body == nil {
			// Ignore empty messages
			// this will also prevent log spam if a queue goes missing
			return nil
		}

		stats.Incr("rabbit-relay-consumer", 1)

		r.log.Debugf("Writing RabbitMQ message to relay channel: %+v", msg)

		relayCh <- &rtypes.RelayMessage{
			Value:   &msg,
			Options: &rtypes.RelayMessageOptions{},
		}

		return nil
	})

MAIN:
	for {
		select {
		case errRabbit := <-errCh:
			util.WriteError(r.log, errorCh, errRabbit.Error)
			stats.IncrPromCounter("plumber_read_errors", 1)
		// TODO: This should probably also listen to the shutdown ctx
		case <-ctx.Done():
			r.log.Info("context cancelled")
			break MAIN
		}
	}

	r.log.Debug("relayer exiting")

	return nil
}

func validateRelayOptions(opts *options.Options) error {
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

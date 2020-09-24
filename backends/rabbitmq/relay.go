package rabbitmq

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
)

type Relayer struct {
	Options *cli.Options
	Channel *amqp.Channel
	MsgDesc *desc.MessageDescriptor
	RelayCh  chan interface{}
	log     *logrus.Entry
}

func Relay(opts *cli.Options) error {
	if err := validateRelayOptions(opts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	// Create new relayer instance (+ validate token & gRPC address)
	relayCfg := &relay.Config{
		Token:       opts.RelayToken,
		GRPCAddress: opts.RelayGRPCAddress,
		NumWorkers:  opts.RelayNumWorkers,
		Timeout:     opts.RelayGRPCTimeout,
		RelayCh:     make(chan interface{}, 1),
		DisableTLS:  opts.RelayGRPCDisableTLS,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Create new service
	channel, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new RabbitMQ service")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	// Launch gRPC Relayer
	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	r := &Relayer{
		Channel:  channel,
		Options:  opts,
		RelayCh:  relayCfg.RelayCh,
		log:      logrus.WithField("pkg", "rabbitmq/relay"),
	}

	return r.Relay()
}

func validateRelayOptions(opts *cli.Options) error {
	return nil
}

func (r *Relayer) Relay() error {
	r.log.Infof("Relaying RabbitMQ messages from '%s' exchange -> '%s'",
		r.Options.Rabbit.Exchange, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	for {

		err := r.Channel.ExchangeDeclare(
			r.Options.Rabbit.Exchange,
			"topic",
			true,
			true,
			false,
			false,
			nil)

		if err != nil {
			return errors.Wrap(err, "could not declare RabbitMQ exchange")
		}

		q, err := r.Channel.QueueDeclare(
			"",
			false,
			false,
			true,
			false,
			nil)

		if err != nil {
			return errors.Wrap(err, "could not declare RabbitMQ queue")
		}

		err = r.Channel.QueueBind(
			q.Name,
			"routing key here",
			r.Options.Rabbit.Exchange,
			false,
			nil)

		msgs, err := r.Channel.Consume(
			q.Name,
			"",
			true,
			false,
			false,
			false,
			nil)

		// Send message(s) to relayer
		for msg := range msgs {
			r.log.Debug("Writing RabbitMQ message to relay channel")

			r.RelayCh <- &types.RelayMessage{
				Value: &msg,
				Options: &types.RelayMessageOptions{},
			}
		}
	}

	return nil
}

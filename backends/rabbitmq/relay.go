package rabbitmq

import (
	"context"
	"fmt"
	"github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/rabbit"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
)

type Relayer struct {
	Options        *cli.Options
	Channel        *amqp.Channel
	MsgDesc        *desc.MessageDescriptor
	RelayCh        chan interface{}
	log            *logrus.Entry
	Looper         *director.FreeLooper
	DefaultContext context.Context
}

func Relay(opts *cli.Options) error {
	if err := validateRelayOptions(opts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	// TODO: move this up the chain?
	ctx := context.Background()

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
		Options:        opts,
		RelayCh:        relayCfg.RelayCh,
		log:            logrus.WithField("pkg", "rabbitmq/relay"),
		Looper:         director.NewFreeLooper(director.FOREVER, make(chan error)),
		DefaultContext: ctx,
	}

	return r.Relay()
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
		URL:          r.Options.Rabbit.Address,
		QueueName:    r.Options.Rabbit.ReadQueue,
		ExchangeName: r.Options.Rabbit.Exchange,
		RoutingKey:   r.Options.Rabbit.RoutingKey,
		AutoAck:      r.Options.Rabbit.ReadAutoAck,
		QueueDeclare: r.Options.Rabbit.ReadQueueDeclare,
	})

	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	ctx, cancel := context.WithCancel(context.Background())

	go rmq.Consume(ctx, errCh, func(msg amqp.Delivery) error {
		r.log.Debug(fmt.Printf("Writing RabbitMQ message to relay channel: %+v", msg))
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
		}
	}

	cancel()
	return nil
}

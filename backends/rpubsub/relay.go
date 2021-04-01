package rpubsub

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/rpubsub/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client         *redis.Client
	Options        *cli.Options
	MsgDesc        *desc.MessageDescriptor
	RelayCh        chan interface{}
	log            *logrus.Entry
	Looper         *director.FreeLooper
	DefaultContext context.Context
}

type IRedisRelayer interface {
	Relay() error
}

var (
	errMissingChannel = errors.New("You must specify at least one channel")
)

// Relay sets up a new RedisPubSub relayer, starts GRPC workers and the API server
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

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &Relayer{
		Client:         client,
		Options:        opts,
		RelayCh:        relayCfg.RelayCh,
		log:            logrus.WithField("pkg", "redis/relay"),
		Looper:         director.NewFreeLooper(director.FOREVER, make(chan error)),
		DefaultContext: context.Background(),
	}

	return r.Relay()
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *cli.Options) error {
	if len(opts.RedisPubSub.Channels) == 0 {
		return errMissingChannel
	}

	// RedisPubSub either supports a password (v1+) OR a username+password (v6+)
	if opts.RedisPubSub.Username != "" && opts.RedisPubSub.Password == "" {
		return errors.New("missing password (either use only password or fill out both)")
	}

	return nil
}

// Relay reads messages from RedisPubSub and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying RedisPubSub messages from %d channel(s) (%s) -> '%s'",
		len(r.Options.RedisPubSub.Channels), r.Options.RedisPubSub.Channels, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	defer r.Client.Close()

	sub := r.Client.Subscribe(r.DefaultContext, r.Options.RedisPubSub.Channels...)
	defer sub.Unsubscribe(r.DefaultContext, r.Options.RedisPubSub.Channels...)

	for {
		msg, err := sub.ReceiveMessage(r.DefaultContext)
		if err != nil {
			// Temporarily mute stats
			stats.Mute("redis-pubsub-relay-consumer")
			stats.Mute("redis-pubsub-relay-producer")

			r.log.Errorf("Unable to read message: %s (retrying in %s)", err, RetryReadInterval)

			time.Sleep(RetryReadInterval)

			continue
		}

		stats.Incr("redis-pubsub-relay-consumer", 1)

		r.log.Debugf("Relaying message received on channel '%s' to Batch (contents: %s)",
			msg.Channel, msg.Payload)

		r.RelayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}
	}
}

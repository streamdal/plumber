package rpubsub

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/rpubsub/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client          *redis.Client
	Options         *cli.Options
	RelayCh         chan interface{}
	log             *logrus.Entry
	Looper          *director.FreeLooper
	ShutdownContext context.Context
}

var (
	ErrMissingChannel = errors.New("You must specify at least one channel")
)

// Relay sets up a new RedisPubSub relayer
func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	client, err := NewClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	return &Relayer{
		Client:          client,
		Options:         opts,
		RelayCh:         relayCh,
		log:             logrus.WithField("pkg", "rpubsub/relay"),
		Looper:          director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownContext: shutdownCtx,
	}, nil
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *cli.Options) error {
	if len(opts.RedisPubSub.Channels) == 0 {
		return ErrMissingChannel
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

	sub := r.Client.Subscribe(r.ShutdownContext, r.Options.RedisPubSub.Channels...)
	defer sub.Unsubscribe(r.ShutdownContext, r.Options.RedisPubSub.Channels...)

	for {
		msg, err := sub.ReceiveMessage(r.ShutdownContext)
		if err != nil {
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

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

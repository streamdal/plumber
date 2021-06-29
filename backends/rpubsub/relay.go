package rpubsub

import (
	"context"
	"strings"
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
	Client      *redis.Client
	Options     *cli.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
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
		Client:      client,
		Options:     opts,
		RelayCh:     relayCh,
		log:         logrus.WithField("pkg", "rpubsub/relay"),
		Looper:      director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownCtx: shutdownCtx,
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

	sub := r.Client.Subscribe(r.ShutdownCtx, r.Options.RedisPubSub.Channels...)
	defer sub.Unsubscribe(r.ShutdownCtx, r.Options.RedisPubSub.Channels...)

	for {
		// Redis library is not handling context cancellation, only timeouts. So we must use a timeout here
		// to ensure we eventually receive the context cancellation from ShutdownCtx
		ctx, _ := context.WithTimeout(r.ShutdownCtx, time.Second*5)
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			// When a timeout occurs
			if strings.Contains(err.Error(), "operation was canceled") {
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			// This will happen every loop when the context times out
			if strings.Contains(err.Error(), "i/o timeout") {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			// Temporarily mute stats
			stats.Mute("redis-pubsub-relay-consumer")
			stats.Mute("redis-pubsub-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

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

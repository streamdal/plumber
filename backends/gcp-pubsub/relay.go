package gcppubsub

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/gcp-pubsub/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client      *pubsub.Client
	Options     *cli.Options
	RelayCh     chan interface{}
	ShutdownCtx context.Context
	log         *logrus.Entry
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {

	// Create new service
	client, err := NewClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new GCP pubsub service")
	}

	return &Relayer{
		Client:      client,
		Options:     opts,
		RelayCh:     relayCh,
		ShutdownCtx: shutdownCtx,
		log:         logrus.WithField("pkg", "gcp-pubsub/relay"),
	}, nil
}

func (r *Relayer) Relay() error {
	r.log.Infof("Relaying GCP pubsub messages from '%s' queue -> '%s'",
		r.Options.GCPPubSub.ReadSubscriptionId, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	defer r.Client.Close()

	sub := r.Client.Subscription(r.Options.GCPPubSub.ReadSubscriptionId)

	// Receive launches several goroutines to exec func, need to use a mutex
	var m sync.Mutex

	var readFunc = func(ctx context.Context, msg *pubsub.Message) {
		m.Lock()
		defer m.Unlock()

		if r.Options.GCPPubSub.ReadAck {
			defer msg.Ack()
		}

		stats.Incr("gcp-pubsub-relay-consumer", 1)

		r.log.Debug("Writing message to relay channel")

		r.RelayCh <- &types.RelayMessage{
			Value: msg,
		}
	}

	for {
		if err := sub.Receive(r.ShutdownCtx, readFunc); err != nil {
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			stats.Mute("gcp-relay-consumer")
			stats.Mute("gcp-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

			r.log.WithField("err", err).Error("unable to read message(s) from GCP pubsub")
			time.Sleep(RetryReadInterval)
			continue
		}
	}

	return nil
}

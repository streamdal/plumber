package gcppubsub

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	gtypes "github.com/batchcorp/plumber/backends/gcp-pubsub/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	ptypes "github.com/batchcorp/plumber/types"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client      *pubsub.Client
	Options     *options.Options
	RelayCh     chan interface{}
	ErrorCh     chan *ptypes.ErrorMessage
	ShutdownCtx context.Context
	log         *logrus.Entry
}

func (g *GCPPubSub) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *ptypes.ErrorMessage) error {
	r := &Relayer{
		Client:      g.client,
		Options:     g.Options,
		RelayCh:     relayCh,
		ErrorCh:     errorCh,
		ShutdownCtx: ctx,
		log:         logrus.WithField("pkg", "gcp-pubsub/relay"),
	}

	return r.Relay()
}

func (r *Relayer) Relay() error {
	r.log.Infof("Relaying GCP pubsub messages from '%s' queue -> '%s'",
		r.Options.GCPPubSub.ReadSubscriptionId, r.Options.Relay.GRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.Relay.HTTPListenAddress)

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

		r.RelayCh <- &gtypes.RelayMessage{
			Value: msg,
		}
	}

MAIN:
	for {
		select {
		case <-r.ShutdownCtx.Done():
			r.log.Debug("context closed")
			break MAIN
		default:
			// Don't block
		}

		if err := sub.Receive(r.ShutdownCtx, readFunc); err != nil {
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, exiting relayer")
				return nil
			}

			stats.Mute("gcp-relay-consumer")
			stats.Mute("gcp-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

			util.WriteError(r.log, r.ErrorCh, errors.Wrap(err, "unable to read message(s) from GCP PubSub"))

			time.Sleep(RetryReadInterval)
			continue
		}
	}

	r.log.Debug("exiting")

	return nil
}

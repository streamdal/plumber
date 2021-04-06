package gcppubsub

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/gcp-pubsub/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client  *pubsub.Client
	Options *cli.Options
	RelayCh chan interface{}
	log     *logrus.Entry
}

func Relay(opts *cli.Options) error {
	relayCfg := &relay.Config{
		Token:       opts.RelayToken,
		GRPCAddress: opts.RelayGRPCAddress,
		NumWorkers:  opts.RelayNumWorkers,
		Timeout:     opts.RelayGRPCTimeout,
		RelayCh:     make(chan interface{}, 1),
		DisableTLS:  opts.RelayGRPCDisableTLS,
		Type:        opts.RelayType,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Create new service
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new GCP pubsub service")
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
		Client:  client,
		Options: opts,
		RelayCh: relayCfg.RelayCh,
		log:     logrus.WithField("pkg", "gcp-pubsub/relay"),
	}

	return r.Relay()
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
		if err := sub.Receive(context.Background(), readFunc); err != nil {
			stats.Mute("gcp-relay-consumer")
			stats.Mute("gcp-relay-producer")

			r.log.WithField("err", err).Error("unable to read message(s) from GCP pubsub")
			time.Sleep(RetryReadInterval)
			continue
		}
	}

	return nil
}

package kubemq_queue

import (
	"context"
	"github.com/batchcorp/plumber/backends/kubemq-queue/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
)

type Relayer struct {
	Options     *cli.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	return &Relayer{
		Options:     opts,
		RelayCh:     relayCh,
		log:         logrus.WithField("pkg", "rabbitmq/relay"),
		Looper:      director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownCtx: shutdownCtx,
	}, nil
}

func validateRelayOptions(opts *cli.Options) error {
	if opts.KubeMQQueue.Queue == "" {
		return errors.New("You must specify a routing key")
	}

	return nil
}

func (r *Relayer) Relay() error {

	r.log.Infof("Relaying KubeMQ Queue messages from '%s' -> '%s'",
		r.Options.KubeMQQueue.Queue, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	client, err := NewClient(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to initialize kubemq-queue consumer")
	}
	defer func() {
		_ = client.Close()
	}()

	for {
		response, err := client.Poll(r.ShutdownCtx,
			queues_stream.NewPollRequest().
				SetChannel(r.Options.KubeMQQueue.Queue).
				SetMaxItems(1).
				SetAutoAck(false).
				SetWaitTimeout(10000))
		if err != nil {
			return err
		}
		if response.HasMessages() {
			if response.Messages[0].Body == nil {
				// Ignore empty messages
				// this will also prevent log spam if a queue goes missing
				return nil
			}
			stats.Incr("kubemq-queue-relay-consumer", 1)

			r.log.Debugf("Writing KubeMQ Queue message to relay channel: %+v", response.Messages[0])
			r.RelayCh <- &types.RelayMessage{
				Value:   response.Messages[0],
				Options: &types.RelayMessageOptions{},
			}

			select {
			case <-r.ShutdownCtx.Done():
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			default:
				// noop
			}

		}
	}
}

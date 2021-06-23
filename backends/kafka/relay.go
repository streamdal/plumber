package kafka

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/kafka/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Options         *cli.Options
	RelayCh         chan interface{}
	log             *logrus.Entry
	Looper          *director.FreeLooper
	ShutdownContext context.Context
}

var (
	ErrMissingTopic = errors.New("You must specify at least one topic")
)

// Relay sets up a new Kafka relayer
func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	return &Relayer{
		Options:         opts,
		RelayCh:         relayCh,
		log:             logrus.WithField("pkg", "kafka/relay"),
		Looper:          director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownContext: shutdownCtx,
	}, nil
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *cli.Options) error {
	if len(opts.Kafka.Topics) == 0 {
		return ErrMissingTopic
	}
	return nil
}

// Relay reads messages from Kafka and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying Kafka messages from '%s' topic(s) -> '%s'",
		r.Options.Kafka.Topics, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	reader, err := NewReader(r.Options)
	if err != nil {
		return err
	}
	defer reader.Reader.Close()
	defer reader.Conn.Close()

	for {
		msg, err := reader.Reader.ReadMessage(r.ShutdownContext)
		if err != nil {
			// Shutdown cancelled, exit so we don't spam logs with context cancelled errors
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			stats.Mute("kafka-relay-consumer")
			stats.Mute("kafka-relay-producer")

			r.log.Errorf("Unable to read kafka message: %s; retrying in %s", err, RetryReadInterval)
			time.Sleep(RetryReadInterval)

			continue
		}

		stats.Incr("kafka-relay-consumer", 1)

		r.log.Debugf("Writing Kafka message to relay channel: %s", msg.Value)

		r.RelayCh <- &types.RelayMessage{
			Value:   &msg,
			Options: &types.RelayMessageOptions{},
		}
	}
}

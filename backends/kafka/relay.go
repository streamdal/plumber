package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	ktypes "github.com/batchcorp/plumber/backends/kafka/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
)

const (
	RetryReadInterval = 5 * time.Second
)

var (
	ErrMissingTopic = errors.New("You must specify at least one topic")
)

// Relay sets up a new Kafka relayer
func (k *Kafka) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	if err := validateRelayOptions(k.Options); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	k.log.Infof("Relaying Kafka messages from '%s' topic(s) -> '%s'",
		k.Options.Kafka.Topics, k.Options.Relay.GRPCAddress)

	k.log.Infof("HTTP server listening on '%s'", k.Options.Relay.HTTPListenAddress)

	reader, err := NewReader(k.dialer, k.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// Shutdown cancelled, exit so we don't spam logs with context cancelled errors
			if err == context.Canceled {
				k.log.Info("Received shutdown signal, exiting relayer")
				return nil
			}

			stats.Mute("kafka-relay-consumer")
			stats.Mute("kafka-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

			wrappedErr := fmt.Errorf("unable to read kafka message: %s; retrying in %s", err, RetryReadInterval)
			util.WriteError(k.log, errorCh, wrappedErr)

			time.Sleep(RetryReadInterval)

			continue
		}

		stats.Incr("kafka-relay-consumer", 1)

		k.log.Debugf("Writing Kafka message to relay channel: %s", msg.Value)

		relayCh <- &ktypes.RelayMessage{
			Value:   &msg,
			Options: &ktypes.RelayMessageOptions{},
		}
	}
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *options.Options) error {
	if len(opts.Kafka.Topics) == 0 {
		return ErrMissingTopic
	}

	return nil
}

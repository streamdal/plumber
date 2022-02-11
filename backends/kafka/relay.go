package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/kafka/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

const (
	RetryReadInterval = 5 * time.Second
)

// Relay sets up a new Kafka relayer
func (k *Kafka) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	reader, err := NewReaderForRelay(k.dialer, k.connArgs, relayOpts.Kafka.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// Shutdown cancelled, exit so we don't spam logs with context cancelled errors
			if err == context.Canceled {
				k.log.Debug("Received shutdown signal, exiting relayer")
				break
			}

			prometheus.Mute("kafka-relay-consumer")
			prometheus.Mute("kafka-relay-producer")

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			wrappedErr := fmt.Errorf("unable to read kafka message: %s; retrying in %s", err, RetryReadInterval)
			util.WriteError(k.log, errorCh, wrappedErr)

			time.Sleep(RetryReadInterval)

			continue
		}

		prometheus.Incr("kafka-relay-consumer", 1)

		k.log.Debugf("Writing Kafka message to relay channel: %s", msg.Value)

		relayCh <- &types.RelayMessage{
			Value:   &msg,
			Options: &types.RelayMessageOptions{},
		}
	}

	k.log.Debugf("relayer for '%s' exiting", relayOpts.XRelayId)

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Kafka == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Kafka.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(relayOpts.Kafka.Args.Topics) == 0 {
		return ErrMissingTopic
	}

	return nil
}

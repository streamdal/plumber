package rabbitmq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/validate"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/rabbit"

	rtypes "github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/plumber/prometheus"
)

func (r *RabbitMQ) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	consumer, err := r.newRabbitForRead(relayOpts.Rabbit.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create new rabbit consumer")
	}

	defer consumer.Close()

	errCh := make(chan *rabbit.ConsumeError)

	go consumer.Consume(ctx, errCh, func(msg amqp.Delivery) error {

		if msg.Body == nil {
			// Ignore empty messages
			// this will also prevent log spam if a queue goes missing
			return nil
		}

		prometheus.Incr("rabbit-relay-consumer", 1)

		r.log.Debugf("Writing message to relay channel: %s", msg.Body)

		relayCh <- &rtypes.RelayMessage{
			Value:   &msg,
			Options: &rtypes.RelayMessageOptions{},
		}

		return nil
	})

	for {
		select {
		case err := <-errCh:
			errorCh <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               err.Error.Error(),
			}
			prometheus.IncrPromCounter("plumber_read_errors", 1)

		case <-ctx.Done():
			r.log.Info("Received shutdown signal, existing relayer")
			return nil
		}
	}

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Rabbit == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Rabbit.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}

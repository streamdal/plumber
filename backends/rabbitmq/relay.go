package rabbitmq

import (
	"context"
	"regexp"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/rabbit"

	rtypes "github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitMQ) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	var excludeRegexp *regexp.Regexp

	if relayOpts.Rabbit.Args.ExcludeBindingKeyRegex != "" {
		var err error

		excludeRegexp, err = regexp.Compile(relayOpts.Rabbit.Args.ExcludeBindingKeyRegex)
		if err != nil {
			return errors.Wrap(err, "unable to compile exclude regex")
		}
	}

	// Check if nil to allow unit testing injection into struct
	if r.client == nil {
		consumer, err := r.newRabbitForRead(relayOpts.Rabbit.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create new rabbit consumer")
		}

		r.client = consumer
	}

	defer r.client.Close()

	errCh := make(chan *rabbit.ConsumeError)

	go r.client.Consume(ctx, errCh, func(msg amqp.Delivery) error {
		if excludeRegexp != nil && excludeRegexp.Match([]byte(msg.RoutingKey)) {
			r.log.Debugf("consumed message for routing key '%s' matches filter '%s' - skipping",
				msg.RoutingKey, relayOpts.Rabbit.Args.ExcludeBindingKeyRegex)

			return nil
		}

		if msg.Body == nil {
			// Ignore empty messages
			// this will also prevent log spam if a queue goes missing
			return nil
		}

		prometheus.Incr("rabbit-relay-consumer", 1)

		r.log.Debugf("Writing message to relay channel: %s", msg.Body)

		relayCh <- &rtypes.RelayMessage{
			Value: &msg,
			Options: &rtypes.RelayMessageOptions{
				DeadLetter: relayOpts.Rabbit.Args.DeadLetter,
			},
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
			r.log.Debug("Received shutdown signal, exiting relayer")
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

	args := relayOpts.Rabbit.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.ExchangeName == "" {
		return ErrEmptyExchangeName
	}

	if args.QueueName == "" {
		return ErrEmptyQueueName
	}

	if args.BindingKey == "" {
		return ErrEmptyBindingKey
	}

	return nil
}

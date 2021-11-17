package azure_servicebus

import (
	"context"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/azure-servicebus/types"

	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureServiceBus) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOpts(relayOpts); err != nil {
		return errors.Wrap(err, "invalid relay options")
	}

	var handler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		a.log.Debug("Writing message to relay channel")

		// This might be nil if no user properties were sent with the original message
		if msg.UserProperties == nil {
			msg.UserProperties = make(map[string]interface{}, 0)
		}

		// Azure's Message struct does not include this information for some reason
		// Seems like it would be good to have. Prefixed with plumber to avoid collisions with user data
		if relayOpts.AzureServiceBus.Args.Queue != "" {
			msg.UserProperties["plumber_queue"] = relayOpts.AzureServiceBus.Args.Queue
		} else {
			msg.UserProperties["plumber_topic"] = relayOpts.AzureServiceBus.Args.Topic
			msg.UserProperties["plumber_subscription"] = relayOpts.AzureServiceBus.Args.SubscriptionName
		}

		prometheus.Incr("azure-servicebus-relay-consumer", 1)

		relayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}

		defer msg.Complete(ctx)
		return nil
	}

	if relayOpts.AzureServiceBus.Args.Queue != "" {
		return a.relayQueue(ctx, handler, relayOpts)
	}

	return a.relayTopic(ctx, handler, relayOpts)
}

// relayQueue reads messages from an ASB queue
func (a *AzureServiceBus) relayQueue(ctx context.Context, handler servicebus.HandlerFunc, relayOpts *opts.RelayOptions) error {
	queue, err := a.client.NewQueue(relayOpts.AzureServiceBus.Args.Queue)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus queue client")
	}

	defer queue.Close(context.Background())
	for {
		if err := queue.ReceiveOne(ctx, handler); err != nil {
			if err == context.Canceled {
				a.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			return err
		}
	}

	return nil
}

// relayTopic reads messages from an ASB topic using the given subscription name
func (a *AzureServiceBus) relayTopic(ctx context.Context, handler servicebus.HandlerFunc, relayOpts *opts.RelayOptions) error {
	topic, err := a.client.NewTopic(relayOpts.AzureServiceBus.Args.Topic)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus topic client")
	}

	sub, err := topic.NewSubscription(relayOpts.AzureServiceBus.Args.SubscriptionName)
	if err != nil {
		return errors.Wrap(err, "unable to create topic subscription")
	}

	defer sub.Close(context.Background())

	for {
		if err := sub.ReceiveOne(ctx, handler); err != nil {
			if err == context.Canceled {
				a.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			return err
		}
	}

	return nil
}

func validateRelayOpts(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if relayOpts.AzureServiceBus == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := relayOpts.AzureServiceBus.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrQueueOrTopic
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrQueueAndTopic
	}

	return nil
}

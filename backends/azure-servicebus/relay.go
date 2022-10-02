package azure_servicebus

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/azure-servicebus/types"

	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureServiceBus) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, _ chan<- *records.ErrorRecord) error {
	if err := validateRelayOpts(relayOpts); err != nil {
		return errors.Wrap(err, "invalid relay options")
	}

	if relayOpts.AzureServiceBus.Args.Queue != "" {
		return a.relayQueue(ctx, relayOpts, relayCh)
	}

	return a.relayTopic(ctx, relayOpts, relayCh)
}

// relayQueue reads messages from an ASB queue
func (a *AzureServiceBus) relayQueue(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}) error {
	receiver, err := a.client.NewReceiverForQueue(relayOpts.AzureServiceBus.Args.Queue, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus queue client")
	}

	defer func() {
		_ = receiver.Close(context.Background())
	}()

	for {
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			return a.handleReceiverError(err)
		}

		for i := range messages {
			if err = a.handleRelayMessage(ctx, relayOpts, relayCh, receiver, messages[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

// relayTopic reads messages from an ASB topic using the given subscription name
func (a *AzureServiceBus) relayTopic(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}) error {
	receiver, err := a.client.NewReceiverForSubscription(relayOpts.AzureServiceBus.Args.Topic, relayOpts.AzureServiceBus.Args.SubscriptionName, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus subscription client")
	}

	defer func() {
		_ = receiver.Close(context.Background())
	}()

	for {
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			return a.handleReceiverError(err)
		}

		for i := range messages {
			if err = a.handleRelayMessage(ctx, relayOpts, relayCh, receiver, messages[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *AzureServiceBus) handleReceiverError(err error) error {
	if err == context.Canceled {
		a.log.Debug("Received shutdown signal, exiting relayer")
		return nil
	}

	prometheus.IncrPromCounter("plumber_read_errors", 1)

	return err
}

func (a *AzureServiceBus) handleRelayMessage(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, receiver *azservicebus.Receiver, msg *azservicebus.ReceivedMessage) error {
	a.log.Debug("Writing message to relay channel")

	// This might be nil if no user properties were sent with the original message
	if msg.ApplicationProperties == nil {
		msg.ApplicationProperties = make(map[string]interface{}, 0)
	}

	// Azure's Message struct does not include this information for some reason
	// Seems like it would be good to have. Prefixed with plumber to avoid collisions with user data
	if relayOpts.AzureServiceBus.Args.Queue != "" {
		msg.ApplicationProperties["plumber_queue"] = relayOpts.AzureServiceBus.Args.Queue
	} else {
		msg.ApplicationProperties["plumber_topic"] = relayOpts.AzureServiceBus.Args.Topic
		msg.ApplicationProperties["plumber_subscription"] = relayOpts.AzureServiceBus.Args.SubscriptionName
	}

	prometheus.Incr("azure-servicebus-relay-consumer", 1)

	relayCh <- &types.RelayMessage{
		Value:   msg,
		Options: &types.RelayMessageOptions{},
	}

	return receiver.CompleteMessage(ctx, msg, nil)
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

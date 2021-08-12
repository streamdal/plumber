package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
	"github.com/pkg/errors"
)

// Write performs necessary setup and calls AzureServiceBus.Write() to write the actual message
func (a *EventHub) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := writer.ValidateWriteOptions(a.Options, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, msg := range messages {
		if err := a.write(ctx, msg.Value); err != nil {
			util.WriteError(a.log, errorCh, errors.Wrap(err, "unable to write message"))
		}
	}

	a.log.Info("finished writing messages")

	return nil
}

// Write writes a message to a random partition on
func (a *EventHub) write(ctx context.Context, value []byte) error {
	opts := make([]eventhub.SendOption, 0)

	if a.Options.AzureEventHub.MessageID != "" {
		opts = append(opts, eventhub.SendWithMessageID(a.Options.AzureEventHub.MessageID))
	}

	event := eventhub.NewEvent(value)
	if a.Options.AzureEventHub.PartitionKey != "" {
		event.PartitionKey = &a.Options.AzureEventHub.PartitionKey
	}

	return a.client.Send(ctx, event, opts...)
}

// validateWriteOptions ensures the correct CLI options are specified for the write action
func validateWriteOptions(opts *options.Options) error {
	return nil
}

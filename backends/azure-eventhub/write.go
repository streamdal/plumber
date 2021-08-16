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
func (e *EventHub) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := writer.ValidateWriteOptions(e.Options, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, msg := range messages {
		if err := e.write(ctx, msg.Value); err != nil {
			util.WriteError(e.log, errorCh, errors.Wrap(err, "unable to write message"))
		}
	}

	e.log.Info("finished writing messages")

	return nil
}

// Write writes a message to a random partition on
func (e *EventHub) write(ctx context.Context, value []byte) error {
	opts := make([]eventhub.SendOption, 0)

	if e.Options.AzureEventHub.MessageID != "" {
		opts = append(opts, eventhub.SendWithMessageID(e.Options.AzureEventHub.MessageID))
	}

	event := eventhub.NewEvent(value)
	if e.Options.AzureEventHub.PartitionKey != "" {
		event.PartitionKey = &e.Options.AzureEventHub.PartitionKey
	}

	return e.client.Send(ctx, event, opts...)
}

// validateWriteOptions ensures the correct CLI options are specified for the write action
func validateWriteOptions(opts *options.Options) error {
	return nil
}

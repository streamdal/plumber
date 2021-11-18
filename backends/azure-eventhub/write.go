package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureEventHub) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	opts := make([]eventhub.SendOption, 0)

	if writeOpts.AzureEventHub.Args.MessageId != "" {
		opts = append(opts, eventhub.SendWithMessageID(writeOpts.AzureEventHub.Args.MessageId))
	}

	for _, msg := range messages {
		event := eventhub.NewEvent([]byte(msg.Input))
		if writeOpts.AzureEventHub.Args.PartitionKey != "" {
			event.PartitionKey = &writeOpts.AzureEventHub.Args.PartitionKey
		}

		if err := a.client.Send(ctx, event, opts...); err != nil {
			util.WriteError(nil, errorCh, errors.Wrap(err, "unable to send azure eventhub event"))
			continue
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.AzureEventHub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.AzureEventHub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}

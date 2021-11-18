package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureEventHub) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := logrus.WithField("pkg", "azure-eventhub/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(dynamicOpts, "Azure Event Hub")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	sendOpts := make([]eventhub.SendOption, 0)

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:

			event := eventhub.NewEvent(outbound.Blob)
			if dynamicOpts.AzureEventHub.Args.PartitionKey != "" {
				event.PartitionKey = &dynamicOpts.AzureEventHub.Args.PartitionKey
			}

			if err := a.client.Send(ctx, event, sendOpts...); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Azure Event Hub for replay '%s'", outbound.ReplayId)
		case <-ctx.Done():
			llog.Warning("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.AzureEventHub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.AzureEventHub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}

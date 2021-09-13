package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dynamic"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (e *EventHub) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "azure-eventhub/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(e.Options, "Azure Event Hub")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	sendOpts := make([]eventhub.SendOption, 0)

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:

			event := eventhub.NewEvent(outbound.Blob)

			if e.Options.AzureEventHub.PartitionKey != "" {
				event.PartitionKey = &e.Options.AzureEventHub.PartitionKey
			}

			if err := e.client.Send(ctx, event, sendOpts...); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Azure Event Hub for replay '%s'", outbound.ReplayId)
		case <-ctx.Done():
			llog.Warn("received notice to quit on context")
			break MAIN
		}
	}

	llog.Debug("exiting")

	return nil
}

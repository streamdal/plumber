package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	ctx := context.Background()
	llog := logrus.WithField("pkg", "azure-eventhub/dynamic")

	// Start up writer
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create Azure client")
	}

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "Azure Event Hub")
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
			if opts.AzureEventHub.PartitionKey != "" {
				event.PartitionKey = &opts.AzureEventHub.PartitionKey
			}

			if err := client.Send(ctx, event, sendOpts...); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Azure Event Hub for replay '%s'", outbound.ReplayId)
		}
	}
}

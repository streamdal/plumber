package azure_eventhub

import (
	"context"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
)

func (a *EventHub) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(a.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	a.log.Info("Listening for message(s) ...")

	count := 1

	var hasRead bool

	handler := func(c context.Context, event *eventhub.Event) error {
		resultsChan <- &types.ReadMessage{
			Value:      event.Data,
			ReceivedAt: time.Now().UTC(),
			Num:        count,
		}

		count++
		hasRead = true

		return nil
	}

	runtimeInfo, err := a.client.GetRuntimeInformation(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get azure eventhub partition list")
	}

MAIN:
	for {
		for _, partitionID := range runtimeInfo.PartitionIDs {
			// Start receiving messages
			//
			// Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called
			// <- listenerHandle.Done() signals listener has stopped
			// listenerHandle.Err() provides the last error the receiver encountered
			listenerHandle, err := a.client.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
			if err != nil {
				return errors.Wrap(err, "unable to receive message from azure eventhub")
			}

			if !a.Options.Read.Follow && hasRead {
				listenerHandle.Close(ctx)
				break MAIN
			}
		}
	}

	a.log.Debug("read exiting")

	return nil
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *options.Options) error {
	return nil
}

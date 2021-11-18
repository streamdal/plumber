package azure_eventhub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureEventHub) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	var count int64
	var hasRead bool

	handler := func(c context.Context, event *eventhub.Event) error {
		count++
		hasRead = true

		serializedMsg, err := json.Marshal(event)
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
			}
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             event.Data,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_AzureEventHub{
				AzureEventHub: &records.AzureEventHub{
					Id:               event.ID,
					SystemProperties: makeSystemPropertiesMap(event.SystemProperties),
					Value:            event.Data,
				},
			},
		}

		return nil
	}

	runtimeInfo, err := a.client.GetRuntimeInformation(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get azure eventhub partition list")
	}

	a.log.Info("Listening for message(s) ...")

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

			if !readOpts.Continuous && hasRead {
				// Test hack since we can't instantiate "eventhub.receiver{}"
				// if listenerHandle != nil {
				listenerHandle.Close(ctx)
				//}
				return nil
			}
		}
	}

	return nil
}

func makeSystemPropertiesMap(properties *eventhub.SystemProperties) map[string]string {
	if properties == nil {
		return map[string]string{}
	}

	return map[string]string{
		"partiion_key":    util.DerefString(properties.PartitionKey),
		"sequence_number": fmt.Sprintf("%d", util.DerefInt64(properties.SequenceNumber)),
		"offset":          fmt.Sprintf("%d", util.DerefInt64(properties.Offset)),
		"partition_id":    fmt.Sprintf("%d", util.DerefInt16(properties.PartitionID)),
		"enqueued_time":   fmt.Sprintf("%d", util.DerefTime(properties.EnqueuedTime)),
	}
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.AzureEventHub == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.AzureEventHub.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}

package relay

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/gcppubsub/types"
)

func (r *Relay) handleGCP(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToGCPRecords(messages)
	if err != nil {
		return errors.Wrap(err, "unable to convert messages to GCP pubsub records")
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddGCPRecord", func(ctx context.Context) error {
		_, err := client.AddGCPRecord(ctx, &services.GCPRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (r *Relay) validateGCPRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.Value == nil {
		return errors.New("msg.Value cannot be nil")
	}

	return nil
}

// derefIntToInt32 dereferences a pointer that is possibly nil.
// Returns 0 if nil, otherwise the int32 value of the data
func derefIntToInt32(i *int) int32 {
	if i != nil {
		return int32(*i)
	}

	return 0
}

func (r *Relay) convertMessagesToGCPRecords(messages []interface{}) ([]*records.GCPRecord, error) {
	sinkRecords := make([]*records.GCPRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateGCPRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate gcp relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.GCPRecord{
			Id:              relayMessage.Value.ID,
			Data:            relayMessage.Value.Data,
			Attributes:      relayMessage.Value.Attributes,
			PublishTime:     relayMessage.Value.PublishTime.UnixNano(),
			DeliveryAttempt: derefIntToInt32(relayMessage.Value.DeliveryAttempt),
			OrderingKey:     relayMessage.Value.OrderingKey,
		})
	}

	return sinkRecords, nil
}

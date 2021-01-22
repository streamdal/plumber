package relay

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/gcp-pubsub/types"
)

func (r *Relay) handleGCP(ctx context.Context, conn *grpc.ClientConn, msg *types.RelayMessage) error {
	if err := r.validateGCPRelayMessage(msg); err != nil {
		return errors.Wrap(err, "unable to validate GCP pubsub relay message")
	}

	gcpRecord := convertGCPMessageToProtobufRecord(msg.Value)

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddGCPRecord(ctx, &services.GCPRecordRequest{
		Records: []*records.GCPRecord{gcpRecord},
	}); err != nil {
		r.log.Debugf("%+v", gcpRecord)
		return errors.Wrap(err, "unable to complete AddGCPRecord call")
	}
	r.log.Debug("successfully handled GCP pubsub message")
	return nil
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

func convertGCPMessageToProtobufRecord(msg *pubsub.Message) *records.GCPRecord {
	return &records.GCPRecord{
		Id:              msg.ID,
		Data:            msg.Data,
		Attributes:      msg.Attributes,
		PublishTime:     msg.PublishTime.UnixNano(),
		DeliveryAttempt: derefIntToInt32(msg.DeliveryAttempt),
		OrderingKey:     msg.OrderingKey,
	}
}

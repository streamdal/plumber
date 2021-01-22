package relay

import (
	"context"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/azure/types"
)

func (r *Relay) handleAzure(ctx context.Context, conn *grpc.ClientConn, msg *types.RelayMessage) error {
	if err := r.validateAzureRelayMessage(msg); err != nil {
		return errors.Wrap(err, "unable to validate Azure relay message")
	}

	azureRecord := convertAzureMessageToProtobufRecord(msg.Value)

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddAzureRecord(ctx, &services.AzureRecordRequest{
		Records: []*records.AzureSinkRecord{azureRecord},
	}); err != nil {
		r.log.Debugf("%+v", azureRecord)
		return errors.Wrap(err, "unable to complete AddAzureRecord call")
	}
	r.log.Debug("successfully handled azure queue message")
	return nil
}

func (r *Relay) validateAzureRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.Value == nil {
		return errors.New("msg.Value cannot be nil")
	}

	return nil
}

// convertUserPropertiesMap converts a map[string]interface{} to map[string]string
func convertMapStringInterface(p map[string]interface{}) map[string]string {
	props := make(map[string]string, 0)
	for k, v := range p {
		sv, ok := v.(string)
		if !ok {
			continue
		}
		props[k] = sv
	}

	return props
}

func derefTime(t *time.Time) int64 {
	if t == nil {
		return int64(0)
	}

	return t.UnixNano()
}

func derefString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}

func derefInt16(i *int16) int16 {
	if i != nil {
		return *i
	}

	return 0
}

func derefInt64(i *int64) int64 {
	if i != nil {
		return *i
	}

	return 0
}

func convertAzureMessageToProtobufRecord(msg *servicebus.Message) *records.AzureSinkRecord {
	r := &records.AzureSinkRecord{
		ContentType:    msg.ContentType,
		CorrelationId:  msg.CorrelationID,
		Data:           msg.Data,
		DeliveryCount:  msg.DeliveryCount,
		SessionId:      *msg.SessionID,
		GroupSequence:  *msg.GroupSequence,
		Id:             msg.ID,
		Label:          msg.Label,
		ReplyTo:        msg.ReplyTo,
		ReplyToGroupId: msg.ReplyToGroupID,
		To:             msg.To,
		Ttl:            msg.TTL.Nanoseconds(),
		LockToken:      msg.LockToken.String(),
		UserProperties: convertMapStringInterface(msg.UserProperties),
		Format:         0,
	}

	if msg.SystemProperties != nil {
		r.SystemProperties = &records.AzureSystemProperties{
			LockedUntil:            derefTime(msg.SystemProperties.LockedUntil),
			SequenceNumber:         derefInt64(msg.SystemProperties.SequenceNumber),
			PartitionId:            int32(derefInt16(msg.SystemProperties.PartitionID)),
			PartitionKey:           derefString(msg.SystemProperties.PartitionKey),
			EnqueuedTime:           derefTime(msg.SystemProperties.EnqueuedTime),
			DeadLetterSource:       derefString(msg.SystemProperties.DeadLetterSource),
			ScheduledEnqueueTime:   derefTime(msg.SystemProperties.ScheduledEnqueueTime),
			EnqueuedSequenceNumber: derefInt64(msg.SystemProperties.EnqueuedSequenceNumber),
			ViaPartitionKey:        derefString(msg.SystemProperties.ViaPartitionKey),
			Annotations:            convertMapStringInterface(msg.SystemProperties.Annotations),
		}
	}

	return r
}

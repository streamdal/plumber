package relay

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/azure-servicebus/types"
	"github.com/batchcorp/plumber/validate"
)

func (r *Relay) handleAzure(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToAzureSinkRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to azure sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddAzureRecord", func(ctx context.Context) error {
		_, err := client.AddAzureRecord(ctx, &services.AzureRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (r *Relay) validateAzureRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return validate.ErrMissingMsg
	}

	if msg.Value == nil {
		return validate.ErrMissingMsgValue
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

func (r *Relay) convertMessagesToAzureSinkRecords(messages []interface{}) ([]*records.AzureSinkRecord, error) {
	sinkRecords := make([]*records.AzureSinkRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateAzureRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate azure relay message (index: %d): %s", i, err)
		}

		r := &records.AzureSinkRecord{
			ContentType:    relayMessage.Value.ContentType,
			CorrelationId:  relayMessage.Value.CorrelationID,
			Data:           relayMessage.Value.Data,
			DeliveryCount:  relayMessage.Value.DeliveryCount,
			SessionId:      *relayMessage.Value.SessionID,
			GroupSequence:  *relayMessage.Value.GroupSequence,
			Id:             relayMessage.Value.ID,
			Label:          relayMessage.Value.Label,
			ReplyTo:        relayMessage.Value.ReplyTo,
			ReplyToGroupId: relayMessage.Value.ReplyToGroupID,
			To:             relayMessage.Value.To,
			Ttl:            relayMessage.Value.TTL.Nanoseconds(),
			LockToken:      relayMessage.Value.LockToken.String(),
			UserProperties: convertMapStringInterface(relayMessage.Value.UserProperties),
			Format:         0,
		}

		if relayMessage.Value.SystemProperties != nil {
			r.SystemProperties = &records.AzureSystemProperties{
				LockedUntil:            derefTime(relayMessage.Value.SystemProperties.LockedUntil),
				SequenceNumber:         derefInt64(relayMessage.Value.SystemProperties.SequenceNumber),
				PartitionId:            int32(derefInt16(relayMessage.Value.SystemProperties.PartitionID)),
				PartitionKey:           derefString(relayMessage.Value.SystemProperties.PartitionKey),
				EnqueuedTime:           derefTime(relayMessage.Value.SystemProperties.EnqueuedTime),
				DeadLetterSource:       derefString(relayMessage.Value.SystemProperties.DeadLetterSource),
				ScheduledEnqueueTime:   derefTime(relayMessage.Value.SystemProperties.ScheduledEnqueueTime),
				EnqueuedSequenceNumber: derefInt64(relayMessage.Value.SystemProperties.EnqueuedSequenceNumber),
				ViaPartitionKey:        derefString(relayMessage.Value.SystemProperties.ViaPartitionKey),
				Annotations:            convertMapStringInterface(relayMessage.Value.SystemProperties.Annotations),
			}
		}

		sinkRecords = append(sinkRecords, r)
	}

	return sinkRecords, nil
}

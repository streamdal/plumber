package relay

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/kubemq-queue/types"
)

// handleKubeMQ sends a KubeMQ relay message to the GRPC server
func (r *Relay) handleKubeMQ(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToKubeMQRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to kubemq records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddKubeMQRecord", func(ctx context.Context) error {
		_, err := client.AddKubeMQRecord(ctx, &services.KubeMQRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateKubeMQRelayMessage ensures all necessary values are present for a kubemq relay message
func (r *Relay) validateKubeMQRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errMissingMessage
	}

	if msg.Value == nil {
		return errMissingMessageValue
	}

	return nil
}

// convertMessagesToKubeMQRecords creates a queues_stream.QueueMessage from a  which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToKubeMQRecords(messages []interface{}) ([]*records.KubeMQRecord, error) {
	sinkRecords := make([]*records.KubeMQRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateKubeMQRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate KubeMQ relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.KubeMQRecord{
			Id:        relayMessage.Value.MessageID,
			Channel:   relayMessage.Value.Channel,
			ClientId:  relayMessage.Value.ClientID,
			Value:     relayMessage.Value.Body,
			Timestamp: time.Now().UTC().UnixNano(),
			Sequence:  int64(relayMessage.Value.Attributes.Sequence),
		})
	}

	return sinkRecords, nil
}

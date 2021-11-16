package relay

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/mqtt/types"
)

// handleMQTT sends a MQTT relay message to the GRPC server
func (r *Relay) handleMQTT(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToMQTTRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to kafka sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddMQTTRecord", func(ctx context.Context) error {
		_, err := client.AddMQTTRecord(ctx, &services.MQTTRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateMQTTMessage ensures all necessary values are present for a MQTT relay message
func (r *Relay) validateMQTTMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errMissingMessage
	}

	if msg.Value == nil {
		return errMissingMessageValue
	}

	return nil
}

// convertMessagesToMQTTRecords creates a records.MQTTRecord from a MQTT.Message which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToMQTTRecords(messages []interface{}) ([]*records.MQTTRecord, error) {
	sinkRecords := make([]*records.MQTTRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateMQTTMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate MQTT relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.MQTTRecord{
			Id:        uint32(relayMessage.Value.MessageID()),
			Topic:     relayMessage.Value.Topic(),
			Duplicate: relayMessage.Value.Duplicate(),
			Retained:  relayMessage.Value.Retained(),
			Qos:       uint32(relayMessage.Value.Qos()),
			Value:     relayMessage.Value.Payload(),
			Timestamp: time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

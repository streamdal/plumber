package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"google.golang.org/grpc"
	"time"
)

// handleKafka sends a Kafka relay message to the GRPC server
func (r *Relay) handleCdcPostgres(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToGenericRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to kafka sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddGenericRecord", func(ctx context.Context) error {
		_, err := client.AddRecord(ctx, &services.GenericRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateKafkaRelayMessage ensures all necessary values are present for a Kafka relay message
func (r *Relay) validateCdcPostgresRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errMissingMessage
	}

	if msg.Value == nil {
		return errMissingMessageValue
	}

	return nil
}

// convertMessagesToGenericRecords creates a records.GenericRecord from a postgres changeset which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToGenericRecords(messages []interface{}) ([]*records.GenericRecord, error) {
	sinkRecords := make([]*records.GenericRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateCdcPostgresRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate cdc-postgres relay message (index: %d): %s", i, err)
		}

		body, err := json.Marshal(relayMessage.Value)
		if err != nil {
			return nil, fmt.Errorf("could not marshal cdc-postgres change set (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.GenericRecord{
			Body:      body,
			Source:    "postgres",
			Timestamp: time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
)

// handleCdcPostgres sends a cdc-postgres relay message to the GRPC server
func (r *Relay) handleCdcPostgres(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToPostgresRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to generic sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddGenericRecord", func(ctx context.Context) error {
		_, err := client.AddRecord(ctx, &services.GenericRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateCDCPostgresRelayMessage ensures all necessary values are present for a cdc-postgres relay message
func (r *Relay) validateCDCPostgresRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errMissingMessage
	}

	if msg.Value == nil {
		return errMissingMessageValue
	}

	return nil
}

// convertMessagesToPostgresRecords creates a records.GenericRecord from a postgres changeset which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToPostgresRecords(messages []interface{}) ([]*records.GenericRecord, error) {
	sinkRecords := make([]*records.GenericRecord, 0)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateCDCPostgresRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate cdc-postgres relay message (index: %d): %s", i, err)
		}

		body, err := json.Marshal(relayMessage.Value)
		if err != nil {
			return nil, fmt.Errorf("could not marshal cdc-postgres change set (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.GenericRecord{
			Body:      body,
			Source:    hostname,
			Timestamp: time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

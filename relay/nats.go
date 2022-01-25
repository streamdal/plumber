package relay

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/nats/types"
)

func (r *Relay) handleNATS(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToNATSRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to nats sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddNATSRecord", func(ctx context.Context) error {
		_, err := client.AddNATSRecord(ctx, &services.NATSRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (r *Relay) convertMessagesToNATSRecords(messages []interface{}) ([]*records.NATSRecord, error) {
	sinkRecords := make([]*records.NATSRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateNATSMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate NATS relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.NATSRecord{
			Subject:   relayMessage.Value.Subject,
			Value:     relayMessage.Value.Data,
			Timestamp: time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

// validateNATSMessage ensures all necessary values are present for a NATS relay message
func (r *Relay) validateNATSMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return ErrMissingMessage
	}

	if msg.Value == nil {
		return ErrMissingMessageValue
	}

	return nil
}

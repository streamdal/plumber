package relay

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/plumber/backends/nats-jetstream/types"
)

func (r *Relay) handleNATSJetStream(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToNATSJetStreamRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to nats-jetstream sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddNATSJetStreamRecord", func(ctx context.Context) error {
		_, err := client.AddNATSJetStreamRecord(ctx, &services.NATSJetStreamRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (r *Relay) convertMessagesToNATSJetStreamRecords(messages []interface{}) ([]*records.NATSJetStreamRecord, error) {
	sinkRecords := make([]*records.NATSJetStreamRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateNATSJetStreamMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate NATS Streaming relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.NATSJetStreamRecord{
			Stream:    relayMessage.Options.Stream,
			Value:     relayMessage.Value.Data,
			Timestamp: time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

// validateNATSJetStreamMessage ensures all necessary values are present for a NATSJetStream relay message
func (r *Relay) validateNATSJetStreamMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return ErrMissingMessage
	}

	if msg.Value == nil {
		return ErrMissingMessageValue
	}

	if msg.Options == nil {
		return ErrMissingMessageOptions
	}

	return nil
}

package relay

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/streamdal/plumber/backends/nats-streaming/types"
)

func (r *Relay) handleNATSStreaming(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToNATSStreamingRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to nats-streaming records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddNATSStreamingRecord", func(ctx context.Context) error {
		_, err := client.AddNATSStreamingRecord(ctx, &services.NATSStreamingRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (r *Relay) convertMessagesToNATSStreamingRecords(messages []interface{}) ([]*records.NATSStreamingRecord, error) {
	sinkRecords := make([]*records.NATSStreamingRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateNATSStreamingMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate NATS Streaming relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.NATSStreamingRecord{
			Channel:         relayMessage.Options.Channel,
			Value:           relayMessage.Value.Data,
			Sequence:        relayMessage.Value.Sequence,
			Subject:         relayMessage.Value.Subject,
			Redelivered:     relayMessage.Value.Redelivered,
			RedeliveryCount: relayMessage.Value.RedeliveryCount,
			Crc32:           relayMessage.Value.CRC32,
			Timestamp:       time.Now().UTC().UnixNano(),
			ForceDeadLetter: r.DeadLetter,
		})
	}

	return sinkRecords, nil
}

// validateNATSStreamingMessage ensures all necessary values are present for a NATSStreaming relay message
func (r *Relay) validateNATSStreamingMessage(msg *types.RelayMessage) error {
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

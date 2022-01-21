package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/nsq/types"
)

var (
	ErrNSQMissingChannel = errors.New("NSQ channel cannot be empty")
	ErrNSQMissingTopic   = errors.New("NSQ topic cannot be empty")
)

// handleNSQ sends a NSQ relay message to the GRPC server
func (r *Relay) handleNSQ(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	records, err := r.convertMessagesToNSQRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to NSQ records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddNSQRecord", func(ctx context.Context) error {
		_, err := client.AddNSQRecord(ctx, &services.NSQRecordRequest{
			Records: records,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateNSQMessage ensures all necessary values are present for a NSQ relay message
func (r *Relay) validateNSQMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return ErrMissingMessage
	}

	if msg.Value == nil {
		return ErrMissingMessageValue
	}

	if msg.Options.Topic == "" {
		return ErrNSQMissingTopic
	}

	if msg.Options.Channel == "" {
		return ErrNSQMissingChannel
	}

	return nil
}

// convertMessagesToNSQRecords creates a records.NSQRecord from a NSQ.Message which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToNSQRecords(messages []interface{}) ([]*records.NSQRecord, error) {
	sinkRecords := make([]*records.NSQRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateNSQMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate NSQ relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.NSQRecord{
			Id:          fmt.Sprintf("%s", relayMessage.Value.ID),
			Topic:       relayMessage.Options.Topic,
			Channel:     relayMessage.Options.Channel,
			Attempts:    int32(relayMessage.Value.Attempts),
			NsqdAddress: relayMessage.Value.NSQDAddress,
			Value:       relayMessage.Value.Body,
			Timestamp:   time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

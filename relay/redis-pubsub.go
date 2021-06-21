package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/rpubsub/types"
)

// handleRedisPubSub sends a RedisPubSub relay message to the GRPC server
func (r *Relay) handleRedisPubSub(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToRedisPubSubSinkRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to redis sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddRedisRecord", func(ctx context.Context) error {
		_, err := client.AddRedisRecord(ctx, &services.RedisRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateRedisRelayMessage ensures all necessary values are present for a RedisPubSub relay message
func (r *Relay) validateRedisRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errMissingMessage
	}

	if msg.Value == nil {
		return errMissingMessageValue
	}

	return nil
}

// convertRedisMessageToProtobufRecord creates a records.RedisSinkRecord from a redis.Message which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToRedisPubSubSinkRecords(messages []interface{}) ([]*records.RedisRecord, error) {
	sinkRecords := make([]*records.RedisRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateRedisRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate redis relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.RedisRecord{
			Payload:   relayMessage.Value.Payload,
			Channel:   relayMessage.Value.Channel,
			Timestamp: time.Now().UTC().UnixNano(),
		})
	}

	return sinkRecords, nil
}

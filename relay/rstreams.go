package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/streamdal/plumber/backends/rstreams/types"
)

var (
	ErrMissingID         = errors.New("missing ID in relay message")
	ErrMissingKeyName    = errors.New("missing Key in relay message")
	ErrMissingStreamName = errors.New("missing Stream in relay message")
)

// handleRedisPubSub sends a RedisPubSub relay message to the GRPC server
func (r *Relay) handleRedisStreams(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToRedisStreamsSinkRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to redis-stream sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddRedisStreamsRecord", func(ctx context.Context) error {
		_, err := client.AddRedisStreamsRecord(ctx, &services.RedisStreamsRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

// validateRedisRelayMessage ensures all necessary values are present for a RedisPubSub relay message
func (r *Relay) validateRedisStreamsRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return ErrMissingMessage
	}

	if msg.Value == nil {
		return ErrMissingMessageValue
	}

	if msg.ID == "" {
		return ErrMissingID
	}

	if msg.Key == "" {
		return ErrMissingKeyName
	}

	if msg.Stream == "" {
		return ErrMissingStreamName
	}

	return nil
}

// convertRedisMessageToProtobufRecord creates a records.RedisSinkRecord from a redis.Message which can then
// be sent to the GRPC server
func (r *Relay) convertMessagesToRedisStreamsSinkRecords(messages []interface{}) ([]*records.RedisStreamsRecord, error) {
	sinkRecords := make([]*records.RedisStreamsRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			r.log.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
			continue
		}

		if err := r.validateRedisStreamsRelayMessage(relayMessage); err != nil {
			r.log.Errorf("unable to validate redis-streams relay message: %s", err)
			continue
		}

		// Create a sink record
		sinkRecords = append(sinkRecords, &records.RedisStreamsRecord{
			Id:              relayMessage.ID,
			Key:             relayMessage.Key,
			Value:           string(relayMessage.Value),
			Stream:          relayMessage.Stream,
			Timestamp:       time.Now().UTC().UnixNano(),
			ForceDeadLetter: r.DeadLetter,
		})
	}

	return sinkRecords, nil
}

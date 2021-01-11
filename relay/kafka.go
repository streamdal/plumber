package relay

import (
	"context"
	"time"

	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/kafka/types"
)

var (
	errMissingMessage      = errors.New("msg cannot be nil")
	errMissingMessageValue = errors.New("msg.Value cannot be nil")
)

// handleKafka sends a Kafka relay message to the GRPC server
func (r *Relay) handleKafka(ctx context.Context, conn *grpc.ClientConn, msg *types.RelayMessage) error {
	if err := r.validateKafkaRelayMessage(msg); err != nil {
		return errors.Wrap(err, "unable to validate kafka relay message")
	}

	kafkaRecord := convertKafkaMessageToProtobufRecord(msg.Value)

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddKafkaRecord(ctx, &services.KafkaSinkRecordRequest{
		Records: []*records.KafkaSinkRecord{kafkaRecord},
	}); err != nil {
		r.log.Errorf("%+v", kafkaRecord)
		return errors.Wrap(err, "unable to complete AddKafkaRecord call")
	}
	r.log.Debug("successfully handled kafka message")
	return nil
}

// validateKafkaRelayMessage ensures all necessary values are present for a Kafka relay message
func (r *Relay) validateKafkaRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errMissingMessage
	}

	if msg.Value == nil {
		return errMissingMessageValue
	}

	return nil
}

// convertKafkaMessageToProtobufRecord creates a records.KafkaSinkRecord from a kafka.Message which can then
// be sent to the GRPC server
func convertKafkaMessageToProtobufRecord(msg *kafka.Message) *records.KafkaSinkRecord {
	return &records.KafkaSinkRecord{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: time.Now().UTC().UnixNano(),
		Offset:    msg.Offset,
		Partition: int32(msg.Partition),
	}
}

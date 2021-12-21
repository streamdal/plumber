package relay

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"

	"github.com/batchcorp/plumber/backends/kafka/types"
)

var (
	errMissingMessage      = errors.New("msg cannot be nil")
	errMissingMessageValue = errors.New("msg.Value cannot be nil")
)

// handleKafka sends a Kafka relay message to the GRPC server
func (r *Relay) handleKafka(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToKafkaSinkRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to kafka sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddKafkaRecord", func(ctx context.Context) error {
		_, err := client.AddKafkaRecord(ctx, &services.KafkaSinkRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
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
func (r *Relay) convertMessagesToKafkaSinkRecords(messages []interface{}) ([]*records.KafkaSinkRecord, error) {
	sinkRecords := make([]*records.KafkaSinkRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateKafkaRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate kafka relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.KafkaSinkRecord{
			Topic:     relayMessage.Value.Topic,
			Key:       relayMessage.Value.Key,
			Value:     relayMessage.Value.Value,
			Timestamp: time.Now().UTC().UnixNano(),
			Offset:    relayMessage.Value.Offset,
			Partition: int32(relayMessage.Value.Partition),
			Headers:   convertKafkaHeaders(relayMessage.Value.Headers),
		})
	}

	return sinkRecords, nil
}

func convertKafkaHeaders(kafkaHeaders []skafka.Header) []*records.KafkaHeader {
	if len(kafkaHeaders) == 0 {
		return nil
	}

	sinkRecordHeaders := make([]*records.KafkaHeader, 0)

	for _, h := range kafkaHeaders {
		v := string(h.Value)

		// gRPC will fail the call if the value isn't valid utf-8
		// TODO: ship original header value so they can be sent back correctly in a replay
		if !utf8.ValidString(v) {
			v = base64.StdEncoding.EncodeToString(h.Value)
		}

		sinkRecordHeaders = append(sinkRecordHeaders, &records.KafkaHeader{
			Key:   h.Key,
			Value: v,
		})
	}

	return sinkRecordHeaders
}

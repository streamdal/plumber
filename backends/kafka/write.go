package kafka

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber/util"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (k *Kafka) Write(ctx context.Context, writeArgs *args.KafkaWriteArgs, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	writer, err := NewWriter(k.dialer, k.connArgs, writeArgs.Topics...)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Close()

	for _, topic := range writeArgs.Topics {
		for _, msg := range messages {
			if err := k.write(ctx, writer, writeArgs, topic, []byte(writeArgs.Key), []byte(msg.Input)); err != nil {
				util.WriteError(k.log, errorCh, fmt.Errorf("unable to write message to topic '%s': %s", topic, err))
			}
		}
	}

	return nil
}

// Write writes a message to a kafka topic. It is a wrapper for WriteMessages.
func (k *Kafka) write(ctx context.Context, writer *skafka.Writer, writeArgs *args.KafkaWriteArgs, topic string, key, value []byte) error {
	msg := skafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	headers := make([]skafka.Header, 0)

	for headerName, headerValue := range writeArgs.Headers {
		headers = append(headers, skafka.Header{
			Key:   headerName,
			Value: []byte(headerValue),
		})
	}

	if len(headers) != 0 {
		msg.Headers = headers
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		return errors.Wrap(err, "unable to publish message(s)")
	}

	k.log.Infof("Successfully wrote message to topic '%s'", topic)

	return nil
}

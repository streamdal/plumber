package kafka

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (k *Kafka) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	writer, err := NewWriter(k.dialer, k.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Close()

	for _, topic := range k.Options.Kafka.Topics {
		for _, msg := range messages {
			if err := k.write(ctx, writer, topic, []byte(k.Options.Kafka.WriteKey), msg.Value); err != nil {
				util.WriteError(k.log, errorCh, fmt.Errorf("unable to write message to topic '%s': %s", topic, err))
			}
		}
	}

	return nil
}

// Write writes a message to a kafka topic. It is a wrapper for WriteMessages.
func (k *Kafka) write(ctx context.Context, writer *skafka.Writer, topic string, key, value []byte) error {
	msg := skafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	headers := make([]skafka.Header, 0)

	for headerName, headerValue := range k.Options.Kafka.WriteHeader {
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

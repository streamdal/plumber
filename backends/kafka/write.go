package kafka

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (k *Kafka) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to verify write options")
	}

	writer, err := NewWriter(k.dialer, k.connArgs, writeOpts.Kafka.Args.Topics...)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Close()

	for _, topic := range writeOpts.Kafka.Args.Topics {
		for _, msg := range messages {
			if err := k.write(ctx, writer, writeOpts.Kafka.Args, topic, []byte(writeOpts.Kafka.Args.Key), []byte(msg.Input)); err != nil {
				util.WriteError(k.log, errorCh, fmt.Errorf("unable to write message to topic '%s': %s", topic, err))
			}
		}
	}

	return nil
}

func validateWriteOptions(opts *opts.WriteOptions) error {
	if opts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if opts.Kafka == nil {
		return validate.ErrEmptyBackendGroup
	}

	if opts.Kafka.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(opts.Kafka.Args.Topics) == 0 {
		return errors.New("at least one topic must be defined")
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

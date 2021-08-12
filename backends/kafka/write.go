package kafka

import (
	"context"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteMessageFromOptions(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	kafkaWriter, err := NewWriter(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	k := &Kafka{
		Options: opts,
		writer:  kafkaWriter.Writer,
		log:     logrus.WithField("pkg", "kafka/write.go"),
	}

	defer kafkaWriter.Conn.Close()
	defer kafkaWriter.Writer.Close()

	for _, value := range writeValues {
		if err := k.Write([]byte(opts.Kafka.WriteKey), value); err != nil {
			k.log.Error(err)
		}
	}

	return nil
}

// Write writes a message to a kafka topic. It is a wrapper for WriteMessages.
func (k *Kafka) Write(key, value []byte) error {
	msg := skafka.Message{
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

	if err := k.writer.WriteMessages(context.Background(), msg); err != nil {
		return errors.Wrap(err, "unable to publish message(s)")
	}

	k.log.Infof("Successfully wrote message to topic '%s'", k.Options.Kafka.Topics[0])

	return nil
}

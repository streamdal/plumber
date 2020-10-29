package kafka

import (
	"context"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	value, err := writer.GenerateWriteValue(opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	w, err := NewWriter(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	k := &Kafka{
		Options: opts,
		Writer:  w,
		log:     logrus.WithField("pkg", "kafka/write.go"),
	}

	return k.Write([]byte(opts.Kafka.WriteKey), value)
}

// Write writes a message to a kafka topic. It is a wrapper for WriteMessages.
func (k *Kafka) Write(key, value []byte) error {
	if err := k.Writer.WriteMessages(context.Background(), skafka.Message{
		Key:   key,
		Value: value,
	}); err != nil {
		return errors.Wrap(err, "unable to publish message(s)")
	}

	k.log.Infof("Successfully wrote message to topic '%s'", k.Options.Kafka.Topic)

	return nil
}

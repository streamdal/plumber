package kafka

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/serializers"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := validateWriteOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.Kafka.WriteOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.Kafka.WriteProtobufDirs, opts.Kafka.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	value, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	writer, err := NewWriter(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	k := &Kafka{
		Options: opts,
		Writer:  writer,
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

func generateWriteValue(md *desc.MessageDescriptor, opts *cli.Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.Kafka.WriteInputData != "" {
		data = []byte(opts.Kafka.WriteInputData)
	}

	if opts.Kafka.WriteInputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.Kafka.WriteInputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.Kafka.WriteInputFile, readErr)
		}
	}

	// Handle AVRO
	if opts.AvroSchemaFile != "" {
		data, err := serializers.AvroEncode(opts.AvroSchemaFile, data)
		if err != nil {
			return nil, err
		}

		return data, nil
	}

	// Ensure we do not try to operate on a nil md
	if opts.Kafka.WriteOutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.Kafka.WriteInputType == "plain" && opts.Kafka.WriteOutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.Kafka.WriteInputType == "jsonpb" && opts.Kafka.WriteOutputType == "protobuf" {
		var convertErr error

		data, convertErr = convertJSONPBToProtobuf(data, dynamic.NewMessage(md))
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		return data, nil
	}

	// TODO: Input: Base64 Output: Plain
	// TODO: Input: Base64 Output: Protobuf
	// TODO: And a few more combinations ...

	return nil, errors.New("unsupported input/output combination")
}

// Convert jsonpb -> protobuf -> bytes
func convertJSONPBToProtobuf(data []byte, m *dynamic.Message) ([]byte, error) {
	buf := bytes.NewBuffer(data)

	if err := jsonpb.Unmarshal(buf, m); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal data into dynamic message")
	}

	// Now let's encode that into a proper protobuf message
	pbBytes, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal dynamic protobuf message to bytes")
	}

	return pbBytes, nil
}

func validateWriteOptions(opts *cli.Options) error {
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.Kafka.WriteOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.Kafka.WriteProtobufDirs,
			opts.Kafka.WriteProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.Kafka.WriteInputData != "" && opts.Kafka.WriteInputFile != "" {
		return fmt.Errorf("--value and --file cannot both be set")
	}

	if opts.Kafka.WriteInputFile != "" {
		if _, err := os.Stat(opts.Kafka.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--file '%s' does not exist", opts.Kafka.WriteInputFile)
		}
	}

	return nil
}

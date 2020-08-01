package rabbitmq

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/pb"
)

// Write is the entry point function for performing write operations in RabbitMQ.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(c *cli.Context) error {
	opts, err := parseOptions(c)
	if err != nil {
		return errors.Wrap(err, "unable to parse options")
	}

	if err := validateWriteOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.OutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.ProtobufDir, opts.ProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	ch, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &RabbitMQ{
		Options: opts,
		Channel: ch,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "rabbitmq/write.go"),
	}

	msg, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return r.Write(msg)
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (r *RabbitMQ) Write(value []byte) error {
	return r.Channel.Publish(r.Options.ExchangeName, r.Options.RoutingKey, false, false, amqp.Publishing{
		Body: value,
	})
}

func validateWriteOptions(opts *Options) error {
	// If output-type is protobuf, ensure that protobuf flags are set
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.OutputType == "protobuf" {
		if opts.ProtobufDir == "" {
			return errors.New("'--protobuf-dir' must be set when type " +
				"is set to 'protobuf'")
		}

		if opts.ProtobufRootMessage == "" {
			return errors.New("'--protobuf-root-message' must be when " +
				"type is set to 'protobuf'")
		}

		// Does given dir exist?
		if _, err := os.Stat(opts.ProtobufDir); os.IsNotExist(err) {
			return fmt.Errorf("--protobuf-dir '%s' does not exist", opts.ProtobufDir)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.InputData != "" && opts.InputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set (choose one!)")
	}

	if opts.InputFile != "" {
		if _, err := os.Stat(opts.InputFile); os.IsNotExist(err) {
			return fmt.Errorf("--input-file '%s' does not exist", opts.InputFile)
		}
	}

	return nil
}

func generateWriteValue(md *desc.MessageDescriptor, opts *Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.InputData != "" {
		data = []byte(opts.InputData)
	}

	if opts.InputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.InputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.InputFile, readErr)
		}
	}

	// Ensure we do not try to operate on a nil md
	if opts.OutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.InputType == "plain" && opts.OutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.InputType == "jsonpb" && opts.OutputType == "protobuf" {
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

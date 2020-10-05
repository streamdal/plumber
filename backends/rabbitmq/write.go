package rabbitmq

import (
	"bytes"
	"context"
	"fmt"
	"github.com/batchcorp/rabbit"
	"io/ioutil"
	"os"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Write is the entry point function for performing write operations in RabbitMQ.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := validateWriteOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.Rabbit.WriteOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.Rabbit.WriteProtobufDirs, opts.Rabbit.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	fmt.Print

	rmq, err := rabbit.New(&rabbit.Options{
		URL:          opts.Rabbit.Address,
		QueueName:    opts.Rabbit.ReadQueue,
		ExchangeName: opts.Rabbit.Exchange,
		RoutingKey:   opts.Rabbit.RoutingKey,
	})

	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	r := &RabbitMQ{
		Options:  opts,
		Consumer: rmq,
		MsgDesc:  md,
		log:      logrus.WithField("pkg", "rabbitmq/write.go"),
	}

	msg, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	ctx := context.Background()

	return r.Write(ctx, msg)
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (r *RabbitMQ) Write(ctx context.Context, value []byte) error {
	return r.Consumer.Publish(ctx, r.Options.Rabbit.RoutingKey, value)
	//return r.Channel.Publish(r.Options.Rabbit.Exchange, r.Options.Rabbit.RoutingKey, false, false, amqp.Publishing{
	//	Body: value,
	//})
}

func validateWriteOptions(opts *cli.Options) error {
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.Rabbit.WriteOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.Rabbit.WriteProtobufDirs,
			opts.Rabbit.WriteProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.Rabbit.WriteInputData != "" && opts.Rabbit.WriteInputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set (choose one!)")
	}

	if opts.Rabbit.WriteInputFile != "" {
		if _, err := os.Stat(opts.Rabbit.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--input-file '%s' does not exist", opts.Rabbit.WriteInputFile)
		}
	}

	return nil
}

func generateWriteValue(md *desc.MessageDescriptor, opts *cli.Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.Rabbit.WriteInputData != "" {
		data = []byte(opts.Rabbit.WriteInputData)
	}

	if opts.Rabbit.WriteInputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.Rabbit.WriteInputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.Rabbit.WriteInputFile, readErr)
		}
	}

	// Ensure we do not try to operate on a nil md
	if opts.Rabbit.WriteOutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.Rabbit.WriteInputType == "plain" && opts.Rabbit.WriteOutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.Rabbit.WriteInputType == "jsonpb" && opts.Rabbit.WriteOutputType == "protobuf" {
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

package awssqs

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
)

// Write is the entry point function for performing write operations in AWSSQS.
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

	if opts.AWSSQS.WriteOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.AWSSQS.WriteProtobufDirs, opts.AWSSQS.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	svc, queueURL, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new service")
	}

	a := &AWSSQS{
		Options:  opts,
		Service:  svc,
		QueueURL: queueURL,
		MsgDesc:  md,
		log:      logrus.WithField("pkg", "awssqs/read.go"),
	}

	msg, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return a.Write(msg)
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.AWSSQS.WriteDelaySeconds < 0 || opts.AWSSQS.WriteDelaySeconds > 900 {
		return errors.New("--delay-seconds must be between 0 and 900")
	}

	// If output-type is protobuf, ensure that protobuf flags are set
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.AWSSQS.WriteOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.AWSSQS.WriteProtobufDirs,
			opts.AWSSQS.WriteProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	if opts.AWSSQS.WriteInputData == "" && opts.AWSSQS.WriteInputFile == "" {
		return fmt.Errorf("either --input-data or --input-file must be set")
	}

	// InputData and file cannot be set at the same time
	if opts.AWSSQS.WriteInputData != "" && opts.AWSSQS.WriteInputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set (choose one!)")
	}

	if opts.AWSSQS.WriteInputFile != "" {
		if _, err := os.Stat(opts.AWSSQS.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--input-file '%s' does not exist", opts.AWSSQS.WriteInputFile)
		}
	}

	return nil
}

func (a *AWSSQS) Write(value []byte) error {
	input := &sqs.SendMessageInput{
		DelaySeconds: aws.Int64(a.Options.AWSSQS.WriteDelaySeconds),
		MessageBody:  aws.String(string(value)),
		QueueUrl:     aws.String(a.QueueURL),
	}

	for k, v := range a.Options.AWSSQS.WriteAttributes {
		input.MessageAttributes[k] = &sqs.MessageAttributeValue{
			StringValue: aws.String(v),
		}
	}

	if _, err := a.Service.SendMessage(input); err != nil {
		return errors.Wrap(err, "unable to complete message send")
	}

	printer.Print(fmt.Sprintf("Successfully wrote message to AWS queue '%s'", a.Options.AWSSQS.QueueName))

	return nil
}

func generateWriteValue(md *desc.MessageDescriptor, opts *cli.Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.AWSSQS.WriteInputData != "" {
		data = []byte(opts.AWSSQS.WriteInputData)
	}

	if opts.AWSSQS.WriteInputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.AWSSQS.WriteInputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.AWSSQS.WriteInputFile, readErr)
		}
	}

	// Ensure we do not try to operate on a nil md
	if opts.AWSSQS.WriteOutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.AWSSQS.WriteInputType == "plain" && opts.AWSSQS.WriteOutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.AWSSQS.WriteInputType == "jsonpb" && opts.AWSSQS.WriteOutputType == "protobuf" {
		var convertErr error

		data, convertErr = convertJSONPBToProtobuf(data, dynamic.NewMessage(md))
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		encoded := base64.StdEncoding.EncodeToString(data)

		return []byte(encoded), nil
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

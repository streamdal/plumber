package awssqs

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/util"
)

const (
	RetryDuration = time.Duration(10) * time.Second
)

// Read is the entry point function for performing read operations in RabbitMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.AWSSQS.ReadOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.AWSSQS.ReadProtobufDirs, opts.AWSSQS.ReadProtobufRootMessage)
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

	return a.Read()
}

func validateReadOptions(opts *cli.Options) error {
	if opts.AWSSQS.ReadMaxNumMessages < 1 || opts.AWSSQS.ReadMaxNumMessages > 10 {
		return errors.New("--max-num-messages must be between 1 and 10")
	}

	if opts.AWSSQS.ReadWaitTimeSeconds < 0 || opts.AWSSQS.ReadWaitTimeSeconds > 20 {
		return errors.New("--wait-time-seconds must be between 0 and 20")
	}

	if opts.AWSSQS.ReadOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.AWSSQS.ReadProtobufDirs,
			opts.AWSSQS.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}

func (a *AWSSQS) Read() error {
	a.log.Info("Listening for message(s) ...")

	lineNumber := 1

	for {
		msgResult, err := a.Service.ReceiveMessage(&sqs.ReceiveMessageInput{
			// We intentionally do not set VisibilityTimeout as we aren't doing anything special with the message
			WaitTimeSeconds:         aws.Int64(a.Options.AWSSQS.ReadWaitTimeSeconds),
			QueueUrl:                aws.String(a.QueueURL),
			ReceiveRequestAttemptId: aws.String(a.Options.AWSSQS.ReadReceiveRequestAttemptId),
			MaxNumberOfMessages:     aws.Int64(a.Options.AWSSQS.ReadMaxNumMessages),
		})
		if err != nil {
			if !a.Options.AWSSQS.ReadFollow {
				return fmt.Errorf("unable to receive any message(s) from SQS: %s", err)
			}

			printer.Error(fmt.Sprintf("unable to receive any message(s) from SQS: %s (retrying in %s)", err, RetryDuration))
			time.Sleep(RetryDuration)
			continue
		}

		// No messages
		if len(msgResult.Messages) == 0 {
			outputMessage := fmt.Sprintf("Received 0 messages after %d seconds", a.Options.AWSSQS.ReadWaitTimeSeconds)

			if a.Options.AWSSQS.ReadFollow {
				outputMessage = outputMessage + fmt.Sprintf("; retrying in %s", RetryDuration)
				printer.Print(outputMessage)
				time.Sleep(RetryDuration)
				continue
			} else {
				printer.Print(outputMessage)
				break
			}
		}

		// Handle decode + output conversion
		for _, m := range msgResult.Messages {
			data, err := a.convertMessage([]byte(*m.Body))
			if err != nil {
				printer.Error(fmt.Sprintf("unable to convert message: %s", err))
				continue
			}

			str := string(data)

			if a.Options.AWSSQS.ReadLineNumbers {
				str = fmt.Sprintf("%d: ", lineNumber) + str
				lineNumber++
			}

			printer.Print(str)

			// Cleanup
			if a.Options.AWSSQS.ReadAutoDelete {
				if _, err := a.Service.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(a.QueueURL),
					ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					printer.Error(fmt.Sprintf("unable to auto-delete message '%s': %s", *m.MessageId, err))
					continue
				}
			}
		}

		if !a.Options.AWSSQS.ReadFollow {
			break
		}
	}

	a.log.Debug("Reader exiting")

	return nil
}

func (a *AWSSQS) convertMessage(msg []byte) ([]byte, error) {
	// Protobuf bits
	if a.Options.AWSSQS.ReadOutputType == "protobuf" {
		// Our implementation of 'protobuf-over-sqs' encodes protobuf in b64
		plain, err := base64.StdEncoding.DecodeString(string(msg))
		if err != nil {
			return nil, fmt.Errorf("unable to decode base64 to protobuf")
		}

		decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(a.MsgDesc), plain)
		if err != nil {
			return nil, fmt.Errorf("unable to decode protobuf message: %s", err)
		}

		msg = decoded
	}

	var data []byte
	var convertErr error

	switch a.Options.AWSSQS.ReadConvert {
	case "base64":
		_, convertErr = base64.StdEncoding.Decode(data, msg)
	case "gzip":
		data, convertErr = util.Gunzip(msg)
	default:
		data = msg
	}

	if convertErr != nil {
		return nil, errors.Wrap(convertErr, "unable to complete conversion")
	}

	return data, nil
}

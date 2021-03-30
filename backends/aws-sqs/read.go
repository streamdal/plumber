package awssqs

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

const (
	RetryDuration = time.Duration(10) * time.Second
)

// Read is the entry point function for performing read operations in AWS SQS.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ReadProtobufRootMessage != "" {
		md, mdErr = pb.FindMessageDescriptor(opts.ReadProtobufDirs, opts.ReadProtobufRootMessage)
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
		Log:      logrus.WithField("pkg", "awssqs/read.go"),
		Printer:  printer.New(),
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

	// If anything protobuf-related is specified, it's being used
	if opts.ReadProtobufRootMessage != "" || len(opts.ReadProtobufDirs) != 0 {
		if err := cli.ValidateProtobufOptions(
			opts.ReadProtobufDirs,
			opts.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}

func (a *AWSSQS) Read() error {
	a.Log.Info("Listening for message(s) ...")

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
			if !a.Options.ReadFollow {
				return fmt.Errorf("unable to receive any message(s) from SQS: %s", err)
			}

			a.Printer.Error(fmt.Sprintf("unable to receive any message(s) from SQS: %s (retrying in %s)", err, RetryDuration))
			time.Sleep(RetryDuration)
			continue
		}

		// No messages
		if len(msgResult.Messages) == 0 {
			outputMessage := fmt.Sprintf("Received 0 messages after %d seconds", a.Options.AWSSQS.ReadWaitTimeSeconds)

			if a.Options.ReadFollow {
				outputMessage = outputMessage + fmt.Sprintf("; retrying in %s", RetryDuration)
				a.Printer.Print(outputMessage)
				time.Sleep(RetryDuration)
				continue
			} else {
				a.Printer.Print(outputMessage)
				break
			}
		}

		// Handle decode + output conversion
		for _, m := range msgResult.Messages {
			data, err := reader.Decode(a.Options, a.MsgDesc, []byte(*m.Body))
			if err != nil {
				a.Printer.Error(fmt.Sprintf("unable to convert message: %s", err))
				continue
			}

			str := string(data)

			if a.Options.ReadLineNumbers {
				str = fmt.Sprintf("%d: ", lineNumber) + str
				lineNumber++
			}

			a.Printer.Print(str)

			// Cleanup
			if a.Options.AWSSQS.ReadAutoDelete {
				if _, err := a.Service.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(a.QueueURL),
					ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					a.Printer.Error(fmt.Sprintf("unable to auto-delete message '%s': %s", *m.MessageId, err))
					continue
				}
			}
		}

		if !a.Options.ReadFollow {
			break
		}
	}

	a.Log.Debug("Reader exiting")

	return nil
}

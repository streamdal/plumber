package awssqs

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
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
func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	svc, queueURL, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new service")
	}

	a := &AWSSQS{
		Options:  opts,
		service:  svc,
		queueURL: queueURL,
		msgDesc:  md,
		log:      logrus.WithField("pkg", "awssqs/read.go"),
		printer:  printer.New(),
	}

	return a.Read()
}

func validateReadOptions(opts *options.Options) error {
	if opts.AWSSQS.ReadMaxNumMessages < 1 || opts.AWSSQS.ReadMaxNumMessages > 10 {
		return errors.New("--max-num-messages must be between 1 and 10")
	}

	if opts.AWSSQS.ReadWaitTimeSeconds < 0 || opts.AWSSQS.ReadWaitTimeSeconds > 20 {
		return errors.New("--wait-time-seconds must be between 0 and 20")
	}

	return nil
}

func (a *AWSSQS) Read() error {
	a.log.Info("Listening for message(s) ...")

	count := 1

	for {
		msgResult, err := a.service.ReceiveMessage(&sqs.ReceiveMessageInput{
			// We intentionally do not set VisibilityTimeout as we aren't doing anything special with the message
			WaitTimeSeconds:         aws.Int64(a.Options.AWSSQS.ReadWaitTimeSeconds),
			QueueUrl:                aws.String(a.queueURL),
			ReceiveRequestAttemptId: aws.String(a.Options.AWSSQS.ReadReceiveRequestAttemptId),
			MaxNumberOfMessages:     aws.Int64(a.Options.AWSSQS.ReadMaxNumMessages),
		})
		if err != nil {
			if !a.Options.ReadFollow {
				return fmt.Errorf("unable to receive any message(s) from SQS: %s", err)
			}

			a.printer.Error(fmt.Sprintf("unable to receive any message(s) from SQS: %s (retrying in %s)", err, RetryDuration))
			time.Sleep(RetryDuration)
			continue
		}

		// No messages
		if len(msgResult.Messages) == 0 {
			outputMessage := fmt.Sprintf("Received 0 messages after %d seconds", a.Options.AWSSQS.ReadWaitTimeSeconds)

			if a.Options.ReadFollow {
				outputMessage = outputMessage + fmt.Sprintf("; retrying in %s", RetryDuration)
				a.printer.Print(outputMessage)
				time.Sleep(RetryDuration)
				continue
			} else {
				a.printer.Print(outputMessage)
				break
			}
		}

		// Handle decode + output conversion
		for _, m := range msgResult.Messages {
			data, err := reader.Decode(a.Options, a.msgDesc, []byte(*m.Body))
			if err != nil {
				a.printer.Error(fmt.Sprintf("unable to convert message: %s", err))
				continue
			}

			str := string(data)

			str = fmt.Sprintf("%d: ", count) + str
			count++

			a.printer.Print(str)

			// Cleanup
			if a.Options.AWSSQS.ReadAutoDelete {
				if _, err := a.service.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(a.queueURL),
					ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					a.printer.Error(fmt.Sprintf("unable to auto-delete message '%s': %s", *m.MessageId, err))
					continue
				}
			}
		}

		if !a.Options.ReadFollow {
			break
		}
	}

	a.log.Debug("reader exiting")

	return nil
}

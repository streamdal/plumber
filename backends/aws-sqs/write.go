package awssqs

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/writer"
)

const (
	ErrInvalidWriteDelaySeconds = "--delay-seconds must be between 0 and 900"
	ErrUnableToSend             = "unable to complete message send"
	ErrMissingMessageGroupID    = "--message-group-id must be specified when writing to a FIFO queue"
)

// Write is the entry point function for performing write operations in AWSSQS.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
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
		Log:      logrus.WithField("pkg", "awssqs/write.go"),
		Printer:  printer.New(),
	}

	msg, err := writer.GenerateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return a.Write(msg)
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.AWSSQS.WriteDelaySeconds < 0 || opts.AWSSQS.WriteDelaySeconds > 900 {
		return errors.New(ErrInvalidWriteDelaySeconds)
	}

	if strings.HasSuffix(opts.AWSSQS.QueueName, ".fifo") && opts.AWSSQS.WriteMessageGroupID == "" {
		return errors.New(ErrMissingMessageGroupID)
	}

	return nil
}

func (a *AWSSQS) Write(value []byte) error {
	input := &sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(a.Options.AWSSQS.WriteDelaySeconds),
		MessageBody:       aws.String(string(value)),
		QueueUrl:          aws.String(a.QueueURL),
		MessageAttributes: make(map[string]*sqs.MessageAttributeValue, 0),
	}

	// This attribute is required for FIFO queues but cannot be present on requests to non-FIFO queues
	if strings.HasSuffix(a.Options.AWSSQS.QueueName, ".fifo") {
		input.MessageGroupId = aws.String(a.Options.AWSSQS.WriteMessageGroupID)

		// Optional for FIFO queues. Must be specified if queue doesn't have dedup enabled on it
		if a.Options.AWSSQS.WriteMessageDeduplicationID != "" {
			input.MessageDeduplicationId = aws.String(a.Options.AWSSQS.WriteMessageDeduplicationID)
		}
	}

	for k, v := range a.Options.AWSSQS.WriteAttributes {
		input.MessageAttributes[k] = &sqs.MessageAttributeValue{
			StringValue: aws.String(v),
		}
	}

	if len(input.MessageAttributes) == 0 {
		input.MessageAttributes = nil
	}

	if _, err := a.Service.SendMessage(input); err != nil {
		return errors.Wrap(err, ErrUnableToSend)
	}

	a.Printer.Print(fmt.Sprintf("Successfully wrote message to AWS queue '%s'", a.Options.AWSSQS.QueueName))

	return nil
}

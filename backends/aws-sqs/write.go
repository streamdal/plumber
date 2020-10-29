package awssqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in AWSSQS.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
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
		log:      logrus.WithField("pkg", "awssqs/read.go"),
	}

	msg, err := writer.GenerateWriteValue(opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return a.Write(msg)
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.AWSSQS.WriteDelaySeconds < 0 || opts.AWSSQS.WriteDelaySeconds > 900 {
		return errors.New("--delay-seconds must be between 0 and 900")
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

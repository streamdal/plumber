package awssqs

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (a *AWSSQS) Write(_ context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	args := writeOpts.AwsSqs.Args

	queueURL, err := a.getQueueURL(args.QueueName, args.RemoteAccountId)
	if err != nil {
		return errors.Wrap(err, "unable to get queue url")
	}

	for _, msg := range messages {
		if err := a.writeMsg(args, msg.Input, queueURL); err != nil {
			util.WriteError(a.log, errorCh, fmt.Errorf("unable to publish message to queue '%s': %s", args.QueueName, err))
			continue
		}
	}

	return nil
}

func (a *AWSSQS) writeMsg(args *args.AWSSQSWriteArgs, body string, queueURL *string) error {
	input := &sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(args.DelaySeconds),
		MessageBody:       aws.String(body),
		QueueUrl:          queueURL,
		MessageAttributes: make(map[string]*sqs.MessageAttributeValue, 0),
	}

	// This attribute is required for FIFO queues but cannot be present on requests to non-FIFO queues
	if strings.HasSuffix(args.QueueName, ".fifo") {
		input.MessageGroupId = aws.String(args.MessageGroupId)

		// Optional for FIFO queues. Must be specified if queue doesn't have dedupe enabled on it
		if args.MessageDeduplicationId != "" {
			input.MessageDeduplicationId = aws.String(args.MessageDeduplicationId)
		}
	}

	for k, v := range args.Attributes {
		input.MessageAttributes[k] = &sqs.MessageAttributeValue{
			StringValue: aws.String(v),
		}
	}

	if len(input.MessageAttributes) == 0 {
		input.MessageAttributes = nil
	}

	if _, err := a.client.SendMessage(input); err != nil {
		return err
	}

	return nil
}

func (a *AWSSQS) getQueueURL(queueName, remoteAccountID string) (*string, error) {
	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	if remoteAccountID != "" {
		input.QueueOwnerAWSAccountId = aws.String(remoteAccountID)
	}

	resultURL, err := a.client.GetQueueUrl(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return nil, errors.Wrap(aerr, "unable to find queue name")
		}

		return nil, errors.Wrap(err, "unable to get queue URL")
	}

	return resultURL.QueueUrl, nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.AwsSqs == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.AwsSqs.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.AwsSqs.Args.QueueName == "" {
		return ErrMissingQueue
	}

	return nil
}

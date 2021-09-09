package awssqs

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/writer"
)

const (
	ErrInvalidWriteDelaySeconds = "--delay-seconds must be between 0 and 900"
	ErrUnableToSend             = "unable to complete message send"
	ErrMissingMessageGroupID    = "--message-group-id must be specified when writing to a FIFO queue"
)

func (a *AWSSQS) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := writer.ValidateWriteOptions(a.Options, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, msg := range messages {
		if err := a.write(msg.Value); err != nil {
			util.WriteError(a.log, errorCh, err)
		}
	}

	return nil
}

func (a *AWSSQS) write(value []byte) error {
	input := &sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(a.Options.AWSSQS.WriteDelaySeconds),
		MessageBody:       aws.String(string(value)),
		QueueUrl:          aws.String(a.queueURL),
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

	if _, err := a.service.SendMessage(input); err != nil {
		return errors.Wrap(err, ErrUnableToSend)
	}

	// TODO: Where should this go?
	// a.printer.Print(fmt.Sprintf("Successfully wrote message to AWS queue '%s'", a.CLIOptions.AWSSQS.QueueName))

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	if opts.AWSSQS.WriteDelaySeconds < 0 || opts.AWSSQS.WriteDelaySeconds > 900 {
		return errors.New(ErrInvalidWriteDelaySeconds)
	}

	if strings.HasSuffix(opts.AWSSQS.QueueName, ".fifo") && opts.AWSSQS.WriteMessageGroupID == "" {
		return errors.New(ErrMissingMessageGroupID)
	}

	return nil
}

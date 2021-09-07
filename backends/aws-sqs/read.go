package awssqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

const (
	RetryDuration = time.Duration(10) * time.Second
)

func validateReadOptions(opts *options.Options) error {
	if opts.AWSSQS.ReadMaxNumMessages < 1 || opts.AWSSQS.ReadMaxNumMessages > 10 {
		return errors.New("--max-num-messages must be between 1 and 10")
	}

	if opts.AWSSQS.ReadWaitTimeSeconds < 0 || opts.AWSSQS.ReadWaitTimeSeconds > 20 {
		return errors.New("--wait-time-seconds must be between 0 and 20")
	}

	return nil
}

func (a *AWSSQS) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(a.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

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
			if !a.Options.Read.Follow {
				wrappedError := errors.Wrap(err, "unable to receive any message(s) from SQS")
				util.WriteError(a.log, errorChan, wrappedError)

				return wrappedError
			}

			retryErr := fmt.Errorf("unable to receive any message(s) from SQS: %s (retrying in %s)", err, RetryDuration)
			util.WriteError(a.log, errorChan, retryErr)

			time.Sleep(RetryDuration)
			continue
		}

		// No messages
		if len(msgResult.Messages) == 0 {
			outputMessage := fmt.Sprintf("Received 0 messages after %d seconds", a.Options.AWSSQS.ReadWaitTimeSeconds)

			if a.Options.Read.Follow {
				outputMessage = outputMessage + fmt.Sprintf("; retrying in %s", RetryDuration)
				util.WriteError(a.log, errorChan, errors.New(outputMessage))
				time.Sleep(RetryDuration)
				continue
			} else {
				util.WriteError(a.log, errorChan, errors.New(outputMessage))
				break
			}
		}

		// New decode + output conversion
		for _, m := range msgResult.Messages {
			resultsChan <- &types.ReadMessage{
				Value:      []byte(*m.Body),
				ReceivedAt: time.Now().UTC(),
				Num:        count,
				Raw:        m,
			}

			count++

			// Cleanup
			if a.Options.AWSSQS.ReadAutoDelete {
				if _, err := a.service.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(a.queueURL),
					ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					util.WriteError(a.log, errorChan, fmt.Errorf("unable to auto-delete message '%s': %s",
						*m.MessageId, err))
					continue
				}
			}
		}

		if !a.Options.Read.Follow {
			break
		}
	}

	a.log.Debug("reader exiting")

	return nil
}

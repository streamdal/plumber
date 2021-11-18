package awssqs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *AWSSQS) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {

	args := readOpts.Awssqs.Args
	if err := validateReadOptions(args); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	queueURL, err := a.getQueueURL(args.QueueName, args.RemoteAccountId)
	if err != nil {
		return err
	}

	a.log.Info("Listening for message(s) ...")

	var count int64

	for {
		msg, err := a.Client.ReceiveMessage(&sqs.ReceiveMessageInput{
			// We intentionally do not set VisibilityTimeout as we aren't doing anything special with the message
			WaitTimeSeconds:         aws.Int64(args.WaitTimeSeconds),
			QueueUrl:                queueURL,
			ReceiveRequestAttemptId: aws.String(args.ReceiveRequestAttemptId),
			MaxNumberOfMessages:     aws.Int64(args.MaxNumMessages),
		})
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to read SQS message").Error(),
			}
			continue
		}

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
			}
		}

		for _, m := range msg.Messages {
			count++
			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             []byte(*m.Body),
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Awssqs{
					Awssqs: &records.AWSSQS{
						Id:              *m.MessageId,
						Timestamp:       time.Now().UTC().Unix(),
						RecipientHandle: *m.ReceiptHandle,
						Attributes:      convertPointerMap(m.Attributes),
						Value:           []byte(*m.Body),
					},
				},
			}

			// Cleanup
			if args.AutoDelete {
				if _, err := a.Client.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      queueURL,
					ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					errorChan <- &records.ErrorRecord{
						OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
						Error:               errors.Wrapf(err, "unable to auto-delete message '%s'", *m.MessageId).Error(),
					}
					continue
				}
			}
		}

		if !readOpts.Continuous {
			break
		}
	}

	return nil
}

// convertPointerMap converts AWS' map of pointer string values to actual string values
func convertPointerMap(input map[string]*string) map[string]string {
	out := make(map[string]string)

	if input == nil {
		return out
	}

	for k, v := range input {
		out[k] = *v
	}

	return out
}

func validateReadOptions(args *args.AWSSQSReadArgs) error {
	if args.MaxNumMessages < 1 || args.MaxNumMessages > 10 {
		return errors.New("--max-num-messages must be between 1 and 10")
	}

	if args.WaitTimeSeconds < 0 || args.WaitTimeSeconds > 20 {
		return errors.New("--wait-time-seconds must be between 0 and 20")
	}

	if args.QueueName == "" {
		return ErrMissingQueue
	}

	return nil
}

package awssqs

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/util"

	"github.com/batchcorp/plumber/validate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/awssqs/types"

	"github.com/batchcorp/plumber/prometheus"
)

func (a *AWSSQS) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify relay options")
	}

	args := relayOpts.AwsSqs.Args

	queueURL, err := a.getQueueURL(args.QueueName, args.RemoteAccountId)
	if err != nil {
		return errors.Wrap(err, "unable to get queue url")
	}

	for {
		select {
		case <-ctx.Done():
			a.log.Debug("Received shutdown signal, exiting relayer")
			return nil
		default:
			// NOOP
		}
		msg, err := a.client.ReceiveMessage(&sqs.ReceiveMessageInput{
			// We intentionally do not set VisibilityTimeout as we aren't doing anything special with the message
			WaitTimeSeconds:         aws.Int64(args.WaitTimeSeconds),
			QueueUrl:                queueURL,
			ReceiveRequestAttemptId: aws.String(args.ReceiveRequestAttemptId),
			MaxNumberOfMessages:     aws.Int64(args.MaxNumMessages),
		})
		if err != nil {
			errorCh <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to read SQS message").Error(),
			}
			prometheus.IncrPromCounter("plumber_read_errors", 1)
			continue
		}

		for _, m := range msg.Messages {
			relayCh <- &types.RelayMessage{
				Value: m,
				Options: &types.RelayMessageOptions{
					Service:    a.client,
					QueueURL:   util.DerefString(queueURL),
					AutoDelete: args.AutoDelete,
				},
			}
			prometheus.Incr("awssqs-relay-consumer", 1)
		}
	}
}

func validateRelayOptions(opts *opts.RelayOptions) error {
	if opts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if opts.AwsSqs == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := opts.AwsSqs.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.MaxNumMessages < 1 || args.MaxNumMessages > 10 {
		return ErrInvalidMaxNumMessages
	}

	if args.WaitTimeSeconds < 0 || args.WaitTimeSeconds > 20 {
		return ErrInvalidWaitTime
	}

	if args.QueueName == "" {
		return ErrMissingQueue
	}

	return nil
}

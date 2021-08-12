package awssqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	atypes "github.com/batchcorp/plumber/backends/aws-sqs/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	ptypes "github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Options     *options.Options
	Service     *sqs.SQS
	QueueURL    string
	RelayCh     chan interface{}
	ShutdownCtx context.Context
	ErrorCh     chan *ptypes.ErrorMessage
	log         *logrus.Entry
}

func (a *AWSSQS) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *ptypes.ErrorMessage) error {
	if err := validateRelayOptions(a.Options); err != nil {
		return errors.Wrap(err, "unable to verify relay options")
	}

	r := &Relayer{
		Service:     a.service,
		QueueURL:    a.queueURL,
		Options:     a.Options,
		RelayCh:     relayCh,
		ErrorCh:     errorCh,
		ShutdownCtx: ctx,
		log:         a.log,
	}

	return r.Relay()
}

func validateRelayOptions(opts *options.Options) error {
	if opts.AWSSQS.RelayMaxNumMessages < 1 || opts.AWSSQS.RelayMaxNumMessages > 10 {
		return errors.New("RelayMaxNumMessages must be between 1 and 10")
	}

	if opts.AWSSQS.RelayWaitTimeSeconds < 0 || opts.AWSSQS.RelayWaitTimeSeconds > 20 {
		return errors.New("RelayWaitTimeSeconds must be between 0 and 20")
	}

	return nil
}

func (r *Relayer) Relay() error {
	r.log.Infof("Relaying AWS SQS messages from '%s' queue -> '%s'",
		r.Options.AWSSQS.QueueName, r.Options.Relay.GRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.Relay.HTTPListenAddress)

MAIN:
	for {
		select {
		case <-r.ShutdownCtx.Done():
			r.log.Info("Received shutdown signal")
			break MAIN
		default:
			// noop
		}

		// Read message(s) from SQS
		msgResult, err := r.Service.ReceiveMessage(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages:     aws.Int64(r.Options.AWSSQS.RelayMaxNumMessages),
			QueueUrl:                aws.String(r.QueueURL),
			ReceiveRequestAttemptId: aws.String(r.Options.AWSSQS.RelayReceiveRequestAttemptId),
			WaitTimeSeconds:         aws.Int64(r.Options.AWSSQS.RelayWaitTimeSeconds),
		})
		if err != nil {
			stats.Mute("sqs-relay-consumer")
			stats.Mute("sqs-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

			util.WriteError(r.log, r.ErrorCh, errors.Wrap(err, "unable to read message (retrying)"))

			time.Sleep(RetryReadInterval)

			continue
		}

		// Send message(s) to relayer
		for _, v := range msgResult.Messages {
			stats.Incr("aws-sqs-relay-consumer", 1)

			r.log.Debug("Writing message to relay channel")

			r.RelayCh <- &atypes.RelayMessage{
				Value: v,
				Options: &atypes.RelayMessageOptions{
					Service:    r.Service,
					QueueURL:   r.QueueURL,
					AutoDelete: r.Options.AWSSQS.RelayAutoDelete,
				},
			}
		}
	}

	r.log.Debug("exiting")

	return nil
}

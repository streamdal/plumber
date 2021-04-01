package awssqs

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/aws-sqs/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Options  *cli.Options
	Service  *sqs.SQS
	QueueURL string
	RelayCh  chan interface{}
	log      *logrus.Entry
}

func Relay(opts *cli.Options) error {
	if err := validateRelayOptions(opts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	// Create new relayer instance (+ validate token & gRPC address)
	relayCfg := &relay.Config{
		Token:       opts.RelayToken,
		GRPCAddress: opts.RelayGRPCAddress,
		NumWorkers:  opts.RelayNumWorkers,
		Timeout:     opts.RelayGRPCTimeout,
		RelayCh:     make(chan interface{}, 1),
		DisableTLS:  opts.RelayGRPCDisableTLS,
		BatchSize:   opts.RelayBatchSize,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Create new service
	svc, queueURL, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new SQS service")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	// Launch gRPC Relayer
	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	r := &Relayer{
		Service:  svc,
		QueueURL: queueURL,
		Options:  opts,
		RelayCh:  relayCfg.RelayCh,
		log:      logrus.WithField("pkg", "aws-sqs/relay"),
	}

	return r.Relay()
}

func validateRelayOptions(opts *cli.Options) error {
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
		r.Options.AWSSQS.QueueName, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	// TODO: Optionally print out relay and SQS config

	for {
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

			r.log.WithField("err", err).Error("unable to read message(s) from SQS")
			time.Sleep(RetryReadInterval)

			continue
		}

		// Send message(s) to relayer
		for _, v := range msgResult.Messages {
			stats.Incr("sqs-relay-consumer", 1)

			r.log.Debug("Writing message to relay channel")

			r.RelayCh <- &types.RelayMessage{
				Value: v,
				Options: &types.RelayMessageOptions{
					Service:    r.Service,
					QueueURL:   r.QueueURL,
					AutoDelete: r.Options.AWSSQS.RelayAutoDelete,
				},
			}
		}
	}

	return nil
}

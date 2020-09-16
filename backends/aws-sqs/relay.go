package awssqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
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

	relayCh := make(chan interface{}, 1)

	// Create new relayer instance (+ validate token & gRPC address)
	grpcRelayer, err := relay.New(opts.Token, opts.GRPCAddress, relayCh)
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
		if err := api.Start(opts.APIListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	// Launch gRPC Relayer
	go grpcRelayer.Run()

	r := &Relayer{
		Service:  svc,
		QueueURL: queueURL,
		Options:  opts,
		RelayCh:  relayCh,
		log:      logrus.WithField("pkg", "awsqs/relay"),
	}

	return r.Relay()
}

func validateRelayOptions(opts *cli.Options) error {
	return nil
}

func (r *Relayer) Relay() error {
	for {
		// Read message(s) from SQS
		msgResult, err := r.Service.ReceiveMessage(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages:     aws.Int64(r.Options.AWSSQS.RelayMaxNumMessages),
			QueueUrl:                aws.String(r.QueueURL),
			ReceiveRequestAttemptId: aws.String(r.Options.AWSSQS.RelayReceiveRequestAttemptId),
			WaitTimeSeconds:         aws.Int64(r.Options.AWSSQS.RelayWaitTimeSeconds),
		})
		if err != nil {
			r.log.WithField("err", err).Error("unable to read message(s) from SQS")
			continue
		}

		// Send message(s) to relayer
		for _, v := range msgResult.Messages {
			r.log.Info("Writing message to relay channel")

			r.RelayCh <- v
		}
	}

	return nil
}

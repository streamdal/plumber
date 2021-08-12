package awssqs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type AWSSQS struct {
	Options *options.Options

	service  *sqs.SQS
	queueURL string
	msgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
	printer  printer.IPrinter
}

func New(opts *options.Options) (*AWSSQS, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	svc, queueURL, err := newService(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to setup service")
	}

	logrus.Debugf("AWS queueURL: %s", queueURL)

	return &AWSSQS{
		Options:  opts,
		service:  svc,
		queueURL: queueURL,
		log:      logrus.WithField("backend", "awssqs"),
	}, nil
}

// Close sets service to nil. Same as with SNS - there is no "connection" to
// disconnect.
func (a *AWSSQS) Close(ctx context.Context) error {
	a.service = nil
	return nil
}

func (a *AWSSQS) Test(ctx context.Context) error {
	return errors.New("not implemented")
}

func (a *AWSSQS) Lag(ctx context.Context) (*types.LagStats, error) {
	return nil, types.UnsupportedFeatureErr
}

func newService(opts *options.Options) (*sqs.SQS, string, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(opts.AWSSQS.QueueName),
	}

	if opts.AWSSQS.RemoteAccountID != "" {
		input.QueueOwnerAWSAccountId = aws.String(opts.AWSSQS.RemoteAccountID)
	}

	resultURL, err := svc.GetQueueUrl(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return nil, "", errors.Wrap(aerr, "unable to find queue name")
		}

		return nil, "", errors.Wrap(err, "unable to get queue URL")
	}

	return svc, *resultURL.QueueUrl, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.AWSSQS == nil {
		return errors.New("AWSSQS options cannot be nil")
	}

	if opts.AWSSQS.QueueName == "" {
		return errors.New("AWSQS queue name cannot be empty")
	}

	return nil
}

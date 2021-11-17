package awssqs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	awsTypes "github.com/batchcorp/plumber/backends/awssqs/types"
	"github.com/batchcorp/plumber/types"
)

const BackendName = "aws-sqs"

var (
	ErrMissingQueue = errors.New("SQS Queue name cannot be empty")
)

type AWSSQS struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.AWSSQSConn

	Client awsTypes.ISQSAPI
	log    *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*AWSSQS, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	client := sqs.New(sess)

	return &AWSSQS{
		connOpts: connOpts,
		connArgs: connOpts.GetAwssqs(),
		Client:   client,
		log:      logrus.WithField("backend", "awssqs"),
	}, nil

}

func (a *AWSSQS) Name() string {
	return BackendName
}

func (a *AWSSQS) Close(_ context.Context) error {
	// Not needed. AWS clients are REST calls
	return nil
}

func (a *AWSSQS) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return errors.New("connection config cannot be nil")
	}

	if connOpts.Conn == nil {
		return errors.New("connection object in connection config cannot be nil")
	}

	if connOpts.GetAwssns() == nil {
		return errors.New("connection config args cannot be nil")
	}

	return nil
}

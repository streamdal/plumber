package awssqs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "aws-sqs"

var (
	ErrMissingQueue          = errors.New("SQS Queue name cannot be empty")
	ErrInvalidMaxNumMessages = errors.New("max number of messages must be between 1 and 10")
	ErrInvalidWaitTime       = errors.New("wait time seconds must be between 0 and 20")
)

type AWSSQS struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.AWSSQSConn

	client sqsiface.SQSAPI
	log    *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*AWSSQS, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	connArgs := connOpts.GetAwsSqs()

	var sess *session.Session
	var err error

	if connArgs.AwsSecretAccessKey != "" {
		sess, err = session.NewSession(&aws.Config{
			Region:      aws.String(connArgs.AwsRegion),
			Credentials: credentials.NewStaticCredentials(connArgs.AwsAccessKeyId, connArgs.AwsSecretAccessKey, ""),
		})
	} else {
		// Use creds stored in ~/.aws/credentials
		// https://github.com/batchcorp/plumber/issues/218
		sess = session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize aws session")
	}

	client := sqs.New(sess)

	return &AWSSQS{
		connOpts: connOpts,
		connArgs: connArgs,
		client:   client,
		log:      logrus.WithField("backend", BackendName),
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
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetAwsSqs()
	if args == nil {
		return validate.ErrMissingConnArgs
	}

	// No need to go further, if profile is set, assume we don't need other args
	// AWS SDK will pick up the profile creds from ~/.aws/credentials
	if args.AwsProfile != "" {
		return nil
	}

	if args.AwsSecretAccessKey == "" {
		return validate.ErrMissingAWSSecretAccessKey
	}

	if args.AwsRegion == "" {
		return validate.ErrMissingAWSRegion
	}

	if args.AwsAccessKeyId == "" {
		return validate.ErrMissingAWSAccessKeyID
	}

	return nil
}

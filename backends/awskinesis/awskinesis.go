package awskinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const BackendName = "kinesis"

var (
	ErrEmptyPartitionKey = errors.New("partition key cannot be empty")
	ErrEmptyStream       = errors.New("stream cannot be empty")
	ErrEmptyShard        = errors.New("shard cannot be empty")
)

type Kinesis struct {
	connOpts  *opts.ConnectionOptions
	client    kinesisiface.KinesisAPI
	readCount uint64
	log       *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*Kinesis, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	connArgs := connOpts.GetAwsKinesis()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(connArgs.AwsRegion),
		Credentials: credentials.NewStaticCredentials(connArgs.AwsAccessKeyId, connArgs.AwsSecretAccessKey, ""),
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize aws session")
	}

	return &Kinesis{
		connOpts: connOpts,
		client:   kinesis.New(sess),
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (k *Kinesis) Name() string {
	return BackendName
}

func (k *Kinesis) Close(_ context.Context) error {
	// Not needed. AWS clients are REST calls
	return nil
}

func (k *Kinesis) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetAwsKinesis()
	if args == nil {
		return validate.ErrMissingConnArgs
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

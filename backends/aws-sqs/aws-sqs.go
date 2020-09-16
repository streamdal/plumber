package awssqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

type AWSSQS struct {
	Options  *cli.Options
	Service  *sqs.SQS
	QueueURL string
	MsgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}

func NewService(opts *cli.Options) (*sqs.SQS, string, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	queueName := aws.String(opts.AWSSQS.QueueName)

	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queueName,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return nil, "", errors.Wrap(aerr, "unable to find queue name")
		}

		return nil, "", errors.Wrap(err, "unable to get queue URL")
	}

	return svc, *resultURL.QueueUrl, nil
}

package awssqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/plumber/backends/aws-sqs/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type AWSSQS struct {
	Options  *options.Options
	Service  types.ISQSAPI
	QueueURL string
	MsgDesc  *desc.MessageDescriptor
	Log      *logrus.Entry
	Printer  printer.IPrinter
}

func NewService(opts *options.Options) (*sqs.SQS, string, error) {
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

	logrus.Debugf("AWS QueueURL: %s", *resultURL.QueueUrl)

	return svc, *resultURL.QueueUrl, nil
}

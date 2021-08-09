package awssns

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/aws-sns/types"
	"github.com/batchcorp/plumber/options"
)

type AWSSNS struct {
	Options  *options.Options
	Service  types.ISNSAPI
	QueueURL string
	MsgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}

func NewService(opts *options.Options) (*sns.SNS, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return sns.New(sess), nil
}

package awssns

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/aws-sns/types"
	"github.com/batchcorp/plumber/options"
)

type AWSSNS struct {
	Options *options.Options

	service  types.ISNSAPI
	queueURL string
	msgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}

func New(opts *options.Options) (*AWSSNS, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &AWSSNS{
		Options: opts,
		log:     logrus.WithField("backend", "awssns"),
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

func NewService(opts *options.Options) (*sns.SNS, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return sns.New(sess), nil
}

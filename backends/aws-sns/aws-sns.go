package awssns

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/batchcorp/plumber/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	atypes "github.com/batchcorp/plumber/backends/aws-sns/types"
	"github.com/batchcorp/plumber/options"
)

const (
	BackendName = "awssns"
)

type AWSSNS struct {
	Options *options.Options

	service  atypes.ISNSAPI
	queueURL string
	msgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}

func New(opts *options.Options) (*AWSSNS, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to create new session")
	}

	return &AWSSNS{
		Options: opts,
		service: sns.New(sess),
		log:     logrus.WithField("backend", "awssns"),
	}, nil
}
func (a *AWSSNS) Name() string {
	return BackendName
}

// Close is _almost_ a noop - since SNS is accessed via an HTTP API, there is no
// "connection" to close as with a traditional bus. All we do is clear the
// service association - the GC will take care of the rest.
func (a *AWSSNS) Close(_ context.Context) error {
	a.service = nil

	return nil
}

func (a *AWSSNS) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

func (a *AWSSNS) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (a *AWSSNS) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func (a *AWSSNS) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.AWSSNS == nil {
		return errors.New("AWSSNS options cannot be nil")
	}

	if opts.AWSSNS.TopicArn == "" {
		return errors.New("AWSSNS TopicArn cannot be empty")
	}

	if !arn.IsARN(opts.AWSSNS.TopicArn) {
		return fmt.Errorf("'%s' is not a valid ARN", opts.AWSSNS.TopicArn)
	}

	return nil
}

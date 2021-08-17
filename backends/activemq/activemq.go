package activemq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

const (
	BackendName = "activemq"
)

type ActiveMq struct {
	Options *options.Options
	log     *logrus.Entry
}

func New(opts *options.Options) (*ActiveMq, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &ActiveMq{
		Options: opts,
		log:     logrus.WithField("backend", "activemq"),
	}, nil
}

func newConn(ctx context.Context, opts *options.Options) (*stomp.Conn, error) {
	o := func(*stomp.Conn) error {
		return nil
	}

	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	var conn *stomp.Conn
	var err error

	go func() {
		// We are using an aggressive heartbeat because either the library or
		// the activemq server tends to drop connections frequently.
		conn, err = stomp.Dial("tcp", opts.ActiveMq.Address, o,
			stomp.ConnOpt.HeartBeat(5*time.Second, time.Second))
		if err != nil {
			errCh <- errors.Wrap(err, "unable to connect to backend")
		}

		doneCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled before backend connected")
	case err := <-errCh:
		return nil, err
	case <-doneCh:
		break
	}

	return conn, nil
}

func (a *ActiveMq) Name() string {
	return BackendName
}

func (a *ActiveMq) Close(ctx context.Context) error {
	return nil
}

func (a *ActiveMq) Test(ctx context.Context) error {
	return errors.New("not implemented")
}

func (a *ActiveMq) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func (a *ActiveMq) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

// getDestination determines the correct string to pass to stomp.Subscribe()
func (a *ActiveMq) getDestination() string {
	if a.Options.ActiveMq.Topic != "" {
		return "/topic/" + a.Options.ActiveMq.Topic
	}

	return a.Options.ActiveMq.Queue
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.ActiveMq == nil {
		return errors.New("ActiveMQ options cannot be nil")
	}

	return nil
}

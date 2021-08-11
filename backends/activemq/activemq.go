package activemq

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type ActiveMq struct {
	Options *options.Options

	client *stomp.Conn
	log    *logrus.Entry
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

func (a *ActiveMq) Connect(ctx context.Context) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	o := func(*stomp.Conn) error {
		return nil
	}

	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	var conn *stomp.Conn
	var err error

	go func() {
		conn, err = stomp.Dial("tcp", a.Options.ActiveMq.Address, o)
		if err != nil {
			errCh <- errors.Wrap(err, "unable to connect to backend")
		}

		doneCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return errors.New("context cancelled before backend connected")
	case err := <-errCh:
		return err
	case <-doneCh:
		break
	}

	a.client = conn

	return nil
}

func (a *ActiveMq) Disconnect(ctx context.Context) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	go func() {
		if err := a.client.Disconnect(); err != nil {
			errCh <- err
		}

		doneCh <- struct{}{}
	}()

	select {
	case <-doneCh:
		break
	case <-ctx.Done():
		return errors.New("context cancelled before disconnect completed")
	case err := <-errCh:
		return err
	}

	// Reset client
	a.client = nil

	return nil
}

// TODO: Implement
func (a *ActiveMq) Test(ctx context.Context) error {
	return errors.New("not implemented")
}

func (a *ActiveMq) Lag(ctx context.Context) error {
	return errors.New("backend does not support lag stats")
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

// getDestination determines the correct string to pass to stomp.Subscribe()
func (a *ActiveMq) getDestination() string {
	if a.Options.ActiveMq.Topic != "" {
		return "/topic/" + a.Options.ActiveMq.Topic
	}

	return a.Options.ActiveMq.Queue
}

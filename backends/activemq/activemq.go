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

	// TODO: Wrap dial with a context
	conn, err := stomp.Dial("tcp", a.Options.ActiveMq.Address, o)
	if err != nil {
		return errors.Wrap(err, "unable to create activemq client")
	}

	a.client = conn

	return nil
}

func (a *ActiveMq) Disconnect(ctx context.Context) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	// TODO: Wrap disconnect in a context
	if err := a.client.Disconnect(); err != nil {
		return errors.Wrap(err, "unable to disconnect")
	}

	a.client = nil

	return nil
}

// TODO: Implement
func (a *ActiveMq) Test(ctx context.Context) error {
	return nil
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

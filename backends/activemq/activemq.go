package activemq

import (
	"github.com/go-stomp/stomp"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type ActiveMq struct {
	Options *options.Options

	client  *stomp.Conn
	msgDesc *desc.MessageDescriptor
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

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

// NewClient returns a configured instance of stomp.Conn
func NewClient(opts *options.Options) (*stomp.Conn, error) {
	o := func(*stomp.Conn) error {
		return nil
	}

	conn, err := stomp.Dial("tcp", opts.ActiveMq.Address, o)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create activemq client")
	}
	return conn, nil
}

// getDestination determines the correct string to pass to stomp.Subscribe()
func (a *ActiveMq) getDestination() string {
	if a.Options.ActiveMq.Topic != "" {
		return "/topic/" + a.Options.ActiveMq.Topic
	}
	return a.Options.ActiveMq.Queue
}

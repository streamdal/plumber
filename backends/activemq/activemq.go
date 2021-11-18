package activemq

import (
	"context"

	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const BackendName = "activemq"

var (
	ErrTopicOrQueue  = errors.New("you must specify either topic or queue")
	ErrTopicAndQueue = errors.New("you must only specify either a topic or a queue")
)

type ActiveMQ struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.ActiveMQConn
	client   *stomp.Conn
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*ActiveMQ, error) {
	o := func(*stomp.Conn) error {
		return nil
	}

	client, err := stomp.Dial("tcp", connOpts.GetActiveMq().Address, o)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create activemq client")
	}
	return &ActiveMQ{
		connOpts: connOpts,
		connArgs: connOpts.GetActiveMq(),
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (a *ActiveMQ) Name() string {
	return BackendName
}

func (a *ActiveMQ) Close(_ context.Context) error {
	return a.client.Disconnect()
}

func (a *ActiveMQ) Test(_ context.Context) error {
	return types.NotImplementedErr
}

// getDestination determines the correct string to pass to stomp.Subscribe()
//func (a *ActiveMQ) getDestination() string {
//	if a.connArgs.Topic != "" {
//		return "/topic/" + a.Options.ActiveMq.Topic
//	}
//	return a.Options.ActiveMq.Queue
//}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	if connOpts.GetActiveMq() == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

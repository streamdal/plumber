package rabbit_streams

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "rabbitmq-streams"

type RabbitStreams struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.RabbitStreamsConn

	// waitGroup is used because write confirmations/errors are async
	waitGroup *sync.WaitGroup

	// errorCh is used because write errors are async
	errorCh chan *records.ErrorRecord

	client     *stream.Environment
	streamName string
	log        *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*RabbitStreams, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate connection options")
	}

	client, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUri(connOpts.GetRabbitStreams().Dsn).
			SetUser(connOpts.GetRabbitStreams().Username).
			SetPassword(connOpts.GetRabbitStreams().Password).
			SetMaxConsumersPerClient(1).
			SetMaxProducersPerClient(1))

	if err != nil {
		return nil, errors.Wrap(err, "unable to create rabbitmq streams client")
	}

	return &RabbitStreams{
		connOpts:  connOpts,
		connArgs:  connOpts.GetRabbitStreams(),
		client:    client,
		waitGroup: &sync.WaitGroup{},
		log:       logrus.WithField("backend", BackendName),
	}, nil
}

func (r *RabbitStreams) Name() string {
	return BackendName
}

func (r *RabbitStreams) Close(_ context.Context) error {
	r.client.Close()
	return nil
}

func (r *RabbitStreams) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	gcpOpts := connOpts.GetRabbitStreams()
	if gcpOpts == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

//func validateDeclareStreamOptions(opts *opts.ConnectionOptions) error {
//	if !opts.GetRabbitStreams().Re {
//		return nil
//	}
//
//	if opts.RabbitMQStreams.DeclareStreamSize == "" {
//		return ErrMissingDeclareSize
//	}
//
//	return nil
//}

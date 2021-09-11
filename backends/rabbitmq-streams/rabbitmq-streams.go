package rabbitmq_streams

import (
	"context"
	"crypto/tls"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

const (
	BackendName = "rabbit-streams"
)

var (
	ErrMissingDeclareSize = errors.New("You must specify --declare-stream-size if you specify" +
		" the --declare-stream option")
)

type RabbitMQStreams struct {
	Options *options.Options

	client    *stream.Environment
	producer  *stream.Producer
	msgDesc   *desc.MessageDescriptor
	log       *logrus.Entry
	waitGroup *sync.WaitGroup
}

func New(opts *options.Options) (*RabbitMQStreams, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &RabbitMQStreams{
		Options: opts,
		log:     logrus.WithField("backend", "rabbitmq_streams"),
	}, nil
}

func (r *RabbitMQStreams) Name() string {
	return BackendName
}

func (r *RabbitMQStreams) Close(ctx context.Context) error {
	return nil
}

func (r *RabbitMQStreams) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (r *RabbitMQStreams) Dynamic(ctx context.Context) error {
	return types.UnsupportedFeatureErr
}

func (r *RabbitMQStreams) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

func getStreamConfig(opts *options.Options) *stream.EnvironmentOptions {
	streamCfg := stream.NewEnvironmentOptions().
		SetUri(opts.RabbitMQStreams.Address).
		SetUser(opts.RabbitMQStreams.Username).
		SetPassword(opts.RabbitMQStreams.Password).
		SetMaxConsumersPerClient(1).
		SetMaxProducersPerClient(1)

	if opts.RabbitMQStreams.UseTLS {
		streamCfg.SetTLSConfig(&tls.Config{})
	}

	if opts.RabbitMQStreams.SkipVerifyTLS {
		streamCfg.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}

	return streamCfg
}

func newClient(opts *options.Options) (*stream.Environment, error) {

	env, err := stream.NewEnvironment(getStreamConfig(opts))

	if err != nil {
		return nil, errors.Wrap(err, "unable to create rabbitmq streams environment")
	}

	if !opts.RabbitMQStreams.DeclareStream {
		return env, nil
	}

	if err := validateDeclareStreamOptions(opts); err != nil {
		return nil, errors.Wrap(err, "could not validate rabbitmq streams options")
	}

	// Try to declare Stream
	if err := env.DeclareStream(opts.RabbitMQStreams.Stream,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.From(opts.RabbitMQStreams.DeclareStreamSize),
		},
	); err != nil {
		if err == stream.StreamAlreadyExists {
			logrus.Debug("Stream already exists, ignoring --declare-stream")
			return env, nil
		}

		return nil, errors.Wrap(err, "unable to declare rabbitmq stream")
	}

	return env, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.RabbitMQStreams == nil {
		return errors.New("rabbitmq streams options cannot be nil")
	}

	return nil
}

func validateDeclareStreamOptions(opts *options.Options) error {
	if !opts.RabbitMQStreams.DeclareStream {
		return nil
	}

	if opts.RabbitMQStreams.DeclareStreamSize == "" {
		return ErrMissingDeclareSize
	}

	return nil
}

package rabbitmqStreams

import (
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

var (
	ErrMissingDeclareSize = errors.New("You must specify --declare-stream-size if you specify" +
		" the --declare-stream option")
)

type RabbitMQStreams struct {
	Client    *stream.Environment
	Producer  *stream.Producer
	Options   *options.Options
	MsgDesc   *desc.MessageDescriptor
	log       *logrus.Entry
	waitGroup *sync.WaitGroup
}

func NewClient(opts *options.Options) (*stream.Environment, error) {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUri(opts.RabbitMQStreams.Address).
			SetUser(opts.RabbitMQStreams.Username).
			SetPassword(opts.RabbitMQStreams.Password).
			SetMaxConsumersPerClient(1).
			SetMaxProducersPerClient(1))

	if err != nil {
		return nil, errors.Wrap(err, "unable to create rabbitmq streams environment")
	}

	if !opts.RabbitMQStreams.DeclareStream {
		return env, nil
	}

	if err := validateDeclareStreamOptions(opts); err != nil {
		return nil, errors.Wrap(err, "could not validate rabbitmq streams options")
	}

	// Declare Stream
	err = env.DeclareStream(opts.RabbitMQStreams.Stream,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.From(opts.RabbitMQStreams.DeclareStreamSize),
		},
	)
	if err == stream.StreamAlreadyExists {
		logrus.Debug("Stream already exists, ignoring --declare-stream")
		return env, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "unable to declare rabbitmq stream")
	}

	return env, nil
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

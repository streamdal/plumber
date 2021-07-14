package rabbitmqStreams

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

var (
	ErrMissingDeclareSize = errors.New("You must specify --declare-stream-size if you specify" +
		" the --declare-stream option")
)

type RabbitMQStreams struct {
	Client   *stream.Environment
	Producer *stream.Producer
	Options  *cli.Options
	MsgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}

func NewClient(opts *cli.Options) (*stream.Environment, error) {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(opts.RabbitMQStreams.Address).
			SetPort(opts.RabbitMQStreams.Port).
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

	// Declare Stream if it doesn't exist
	streamExists, err := env.StreamExists(opts.RabbitMQStreams.Stream)
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine if stream exists")
	}
	if !streamExists {
		logrus.Debug("Stream doesn't exist, declaring")
		err = env.DeclareStream(opts.RabbitMQStreams.Stream,
			&stream.StreamOptions{
				MaxLengthBytes: stream.ByteCapacity{}.From(opts.RabbitMQStreams.DeclareStreamSize),
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "unable to declare rabbitmq stream")
		}
	} else {
		logrus.Debug("Stream already exists, ignoring --declare-stream")
	}

	return env, nil
}

func validateDeclareStreamOptions(opts *cli.Options) error {
	if !opts.RabbitMQStreams.DeclareStream {
		return nil
	}

	if opts.RabbitMQStreams.DeclareStreamSize == "" {
		return ErrMissingDeclareSize
	}

	return nil
}

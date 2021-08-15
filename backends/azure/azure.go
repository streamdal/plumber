package azure

import (
	"context"

	"github.com/Azure/azure-service-bus-go"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

var (
	errTopicOrQueue        = errors.New("You may only specify a topic or a queue, not both")
	errMissingSubscription = errors.New("You must specify a subscription name when reading from a topic")
)

type ServiceBus struct {
	Options *options.Options

	client *servicebus.Namespace
	queue  *servicebus.Queue
	topic  *servicebus.Topic
	log    *logrus.Entry
}

func New(opts *options.Options) (*ServiceBus, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	sb := &ServiceBus{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "azure"),
	}

	if opts.Azure.Queue != "" {
		queue, err := client.NewQueue(opts.Azure.Queue)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new azure service bus queue client")
		}

		sb.queue = queue
	} else {
		topic, err := client.NewTopic(opts.Azure.Topic)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new azure service bus topic client")
		}

		sb.topic = topic
	}

	return sb, nil
}

func (s *ServiceBus) Close(ctx context.Context) error {
	if s.queue != nil {
		if err := s.queue.Close(ctx); err != nil {
			return errors.Wrap(err, "unable to close queue")
		}
	}

	if s.topic != nil {
		if err := s.topic.Close(ctx); err != nil {
			return errors.Wrap(err, "unable to close topic")
		}
	}

	s.client = nil

	return nil
}

func (s *ServiceBus) Test(_ context.Context) error {
	return errors.New("not implemented")
}

func (s *ServiceBus) Lag(_ context.Context) (*types.Lag, error) {
	return nil, types.UnsupportedFeatureErr
}

// NewClient returns a properly configured service bus client
func newClient(opts *options.Options) (*servicebus.Namespace, error) {
	c, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(opts.Azure.ConnectionString))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure service bus client")
	}
	return c, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.Azure == nil {
		return errors.New("Azure options cannot be nil")
	}

	return nil
}

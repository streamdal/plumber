package azure

import (
	"github.com/Azure/azure-service-bus-go"
	"github.com/jhump/protoreflect/desc"
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

	msgDesc *desc.MessageDescriptor
	client  *servicebus.Namespace
	queue   *servicebus.Queue
	topic   *servicebus.Topic
	log     *logrus.Entry
}

func New(opts *options.Options) (*ServiceBus, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &ServiceBus{
		Options: opts,
		log:     logrus.WithField("backend", "azure"),
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

// NewClient returns a properly configured service bus client
func NewClient(opts *options.Options) (*servicebus.Namespace, error) {
	c, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(opts.Azure.ConnectionString))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure service bus client")
	}
	return c, nil
}

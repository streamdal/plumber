package azure

import (
	"github.com/Azure/azure-service-bus-go"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

var (
	errTopicOrQueue        = errors.New("You may only specify a topic or a queue, not both")
	errMissingSubscription = errors.New("You must specify a subscription name when reading from a topic")
)

type AzureServiceBus struct {
	Options *cli.Options
	MsgDesc *desc.MessageDescriptor
	Client  *servicebus.Namespace
	Queue   *servicebus.Queue
	Topic   *servicebus.Topic
	log     *logrus.Entry
}

// NewClient returns a properly configured service bus client
func NewClient(opts *cli.Options) (*servicebus.Namespace, error) {
	c, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(opts.Azure.ConnectionString))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure service bus client")
	}
	return c, nil
}

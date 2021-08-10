package azure_eventhub

import (
	"github.com/Azure/azure-event-hubs-go/v3"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type EventHub struct {
	Options *options.Options

	msgDesc *desc.MessageDescriptor
	client  *eventhub.Hub
	log     *logrus.Entry
	printer printer.IPrinter
}

func New(opts *options.Options) (*EventHub, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &EventHub{
		Options: opts,
		log:     logrus.WithField("backend", "azure_eventhub"),
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

// NewClient returns a properly configured service bus client
func NewClient(opts *options.Options) (*eventhub.Hub, error) {
	c, err := eventhub.NewHubFromConnectionString(opts.AzureEventHub.ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure event hub client")
	}
	return c, nil
}

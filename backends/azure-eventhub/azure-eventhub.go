package azure_eventhub

import (
	"github.com/Azure/azure-event-hubs-go/v3"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type AzureEventHub struct {
	Options *options.Options
	MsgDesc *desc.MessageDescriptor
	Client  *eventhub.Hub
	log     *logrus.Entry
	printer printer.IPrinter
}

// NewClient returns a properly configured service bus client
func NewClient(opts *options.Options) (*eventhub.Hub, error) {
	c, err := eventhub.NewHubFromConnectionString(opts.AzureEventHub.ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure event hub client")
	}
	return c, nil
}

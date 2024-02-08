package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const BackendName = "azure-eventhub"

type AzureEventHub struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.AzureEventHubConn
	client   *eventhub.Hub //TODO: tools.IEventhub eventually
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*AzureEventHub, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	client, err := eventhub.NewHubFromConnectionString(connOpts.GetAzureEventHub().ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure event hub client")
	}

	return &AzureEventHub{
		connOpts: connOpts,
		connArgs: connOpts.GetAzureEventHub(),
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (a *AzureEventHub) Name() string {
	return BackendName
}

func (a *AzureEventHub) Close(_ context.Context) error {
	return nil
}

func (a *AzureEventHub) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	if connOpts.GetAzureEventHub() == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

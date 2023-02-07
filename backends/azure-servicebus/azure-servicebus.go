package azure_servicebus

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "azure-servicebus"

var (
	ErrQueueOrTopic  = errors.New("either a queue or topic name must be specified")
	ErrQueueAndTopic = errors.New("only one topic or queue can be specified")
)

type messageHandlerFunc func(ctx context.Context, receiver *azservicebus.Receiver, msg *azservicebus.ReceivedMessage) error

type AzureServiceBus struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.AzureServiceBusConn
	client   *azservicebus.Client
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*AzureServiceBus, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	client, err := azservicebus.NewClientFromConnectionString(connOpts.GetAzureServiceBus().ConnectionString, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure service bus client")
	}

	return &AzureServiceBus{
		connOpts: connOpts,
		connArgs: connOpts.GetAzureServiceBus(),
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (a *AzureServiceBus) Name() string {
	return BackendName
}

func (a *AzureServiceBus) Close(_ context.Context) error {
	return nil
}

func (a *AzureServiceBus) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	if connOpts.GetAzureServiceBus() == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

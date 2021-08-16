package azure_eventhub

import (
	"context"
	"time"

	"github.com/Azure/azure-event-hubs-go/v3"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type EventHub struct {
	Options *options.Options

	client *eventhub.Hub
	log    *logrus.Entry
}

func New(opts *options.Options) (*EventHub, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new eventhub client")
	}

	return &EventHub{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "azure_eventhub"),
	}, nil
}

func (a *EventHub) Close(ctx context.Context) error {
	return nil
}

func (a *EventHub) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (a *EventHub) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func (a *EventHub) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

// NewClient returns a properly configured service bus client
func newClient(opts *options.Options) (*eventhub.Hub, error) {
	c, err := eventhub.NewHubFromConnectionString(opts.AzureEventHub.ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new azure event hub client")
	}
	return c, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.AzureEventHub == nil {
		return errors.New("EventHub options cannot be nil")
	}

	return nil
}

package gcppubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

const (
	BackendName = "gcp-pubsub"
)

type GCPPubSub struct {
	Options *options.Options

	client *pubsub.Client
	log    *logrus.Entry
}

func New(opts *options.Options) (*GCPPubSub, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to setup new client")
	}

	return &GCPPubSub{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "gcppubsub"),
	}, nil
}

func (g *GCPPubSub) Name() string {
	return BackendName
}

func (g *GCPPubSub) Close(ctx context.Context) error {
	if g.client == nil {
		return nil
	}

	if err := g.client.Close(); err != nil {
		return errors.Wrap(err, "unable to close client")
	}

	return nil
}

func (g *GCPPubSub) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (g *GCPPubSub) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func newClient(opts *options.Options) (*pubsub.Client, error) {
	c, err := pubsub.NewClient(context.Background(), opts.GCPPubSub.ProjectId)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new pubsub client")
	}

	return c, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.GCPPubSub == nil {
		return errors.New("GCP PubSub options cannot be nil")
	}

	return nil
}

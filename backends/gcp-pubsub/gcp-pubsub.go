package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber/options"
)

const (
	BackendName = "gcp-pubsub"
)

var (
	ErrMissingOptions     = errors.New("options cannot be nil")
	ErrMissingGCPOptions  = errors.New("GCP PubSub options cannot be nil")
	ErrMissingCredentials = errors.New("Either --credentials-file or --credentials-json must be specified")
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

func newClient(opts *options.Options) (*pubsub.Client, error) {

	clientOpts := make([]option.ClientOption, 0)

	// Determine how we are authenticating
	if opts.GCPPubSub.CredentialsJSON != "" {
		// User passed in JSON data
		clientOpts = append(clientOpts, option.WithCredentialsJSON([]byte(opts.GCPPubSub.CredentialsJSON)))
	} else if opts.GCPPubSub.CredentialsFile != "" {
		// User wants to pull credentials from a JSON file
		clientOpts = append(clientOpts, option.WithCredentialsFile(opts.GCPPubSub.CredentialsFile))
	}

	c, err := pubsub.NewClient(context.Background(), opts.GCPPubSub.ProjectId, clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new pubsub client")
	}

	return c, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return ErrMissingOptions
	}

	if opts.GCPPubSub == nil {
		return ErrMissingGCPOptions
	}

	if opts.GCPPubSub.CredentialsJSON == "" && opts.GCPPubSub.CredentialsFile == "" {
		return ErrMissingCredentials
	}

	return nil
}

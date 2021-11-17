package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "gcp-pubsub"

type GCPPubSub struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.GCPPubSubConn

	client *pubsub.Client

	log *logrus.Entry
}

func New(opts *opts.ConnectionOptions) (*GCPPubSub, error) {
	if err := validateBaseConnOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate connection options")
	}

	clientOpts := make([]option.ClientOption, 0)

	// Determine how we are authenticating
	if opts.GetGcpPubsub().CredentialsJson != "" {
		// User passed in JSON data
		clientOpts = append(clientOpts, option.WithCredentialsJSON([]byte(opts.GetGcpPubsub().CredentialsJson)))
	} else if opts.GetGcpPubsub().CredentialsFile != "" {
		// User wants to pull credentials from a JSON file
		clientOpts = append(clientOpts, option.WithCredentialsFile(opts.GetGcpPubsub().CredentialsFile))
	}

	client, err := pubsub.NewClient(context.TODO(), opts.GetGcpPubsub().ProjectId, clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create GCP connection")
	}

	return &GCPPubSub{
		connOpts: opts,
		connArgs: opts.GetGcpPubsub(),
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (g *GCPPubSub) Name() string {
	return BackendName
}

func (g *GCPPubSub) Close(_ context.Context) error {
	g.client.Close()
	return nil
}

func (g *GCPPubSub) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	gcpOpts := connOpts.GetGcpPubsub()
	if gcpOpts == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

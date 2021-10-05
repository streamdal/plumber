package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/batchcorp/plumber/cli"
)

var (
	ErrMissingOptions     = errors.New("options cannot be nil")
	ErrMissingGCPOptions  = errors.New("GCP PubSub options cannot be nil")
	ErrMissingCredentials = errors.New("Either --credentials-file or --credentials-json must be specified")
)

type GCPPubSub struct {
	Options *cli.Options
	MsgDesc *desc.MessageDescriptor
	Client  *pubsub.Client
	log     *logrus.Entry
}

func NewClient(opts *cli.Options) (*pubsub.Client, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

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

func validateOpts(opts *cli.Options) error {
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

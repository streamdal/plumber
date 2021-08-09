package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type GCPPubSub struct {
	Options *options.Options
	MsgDesc *desc.MessageDescriptor
	Client  *pubsub.Client
	log     *logrus.Entry
}

func NewClient(opts *options.Options) (*pubsub.Client, error) {
	c, err := pubsub.NewClient(context.Background(), opts.GCPPubSub.ProjectId)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new pubsub client")
	}

	return c, nil
}

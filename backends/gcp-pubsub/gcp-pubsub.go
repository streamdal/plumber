package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"google.golang.org/api/option"
)

type Options struct {
	ProjectId           string
	SubscriptionId      string
	APIKey              string
	OutputType          string
	ProtobufDir         string
	ProtobufRootMessage string
	Follow              bool
	Convert             string
	LineNumbers         bool
}

type GCPPubSub struct {
	Options *Options
	MsgDesc *desc.MessageDescriptor
	Client  *pubsub.Client
	log     *logrus.Entry
}

func parseOptions(c *cli.Context) (*Options, error) {
	return &Options{
		ProjectId:           c.String("project-id"),
		SubscriptionId:      c.String("subscription-id"),
		APIKey:              c.String("api-key"),
		OutputType:          c.String("output-type"),
		Convert:             c.String("convert"),
		ProtobufDir:         c.String("protobuf-dir"),
		ProtobufRootMessage: c.String("protobuf-root-message"),
		Follow:              c.Bool("follow"),
		LineNumbers:         c.Bool("line-numbers"),
	}, nil
}

func NewClient(opts *Options) (*pubsub.Client, error) {
	var clientOptions []option.ClientOption

	if opts.APIKey != "" {
		clientOptions = append(clientOptions, option.WithAPIKey(opts.APIKey))
	}

	c, err := pubsub.NewClient(context.Background(), opts.ProjectId, clientOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new pubsub client")
	}

	return c, nil
}

package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

type Options struct {
	ProjectId           string
	SubscriptionId      string
	TopicId             string
	OutputType          string
	ProtobufDir         string
	ProtobufRootMessage string
	Follow              bool
	Convert             string
	LineNumbers         bool
	Ack                 bool
	InputData           string
	InputFile           string
	InputType           string
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
		SubscriptionId:      c.String("sub-id"),
		TopicId:             c.String("topic-id"),
		OutputType:          c.String("output-type"),
		Convert:             c.String("convert"),
		ProtobufDir:         c.String("protobuf-dir"),
		ProtobufRootMessage: c.String("protobuf-root-message"),
		InputData:           c.String("input-data"),
		InputFile:           c.String("input-file"),
		InputType:           c.String("input-type"),
		Follow:              c.Bool("follow"),
		LineNumbers:         c.Bool("line-numbers"),
		Ack:                 c.Bool("ack"),
	}, nil
}

func NewClient(opts *Options) (*pubsub.Client, error) {
	c, err := pubsub.NewClient(context.Background(), opts.ProjectId)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new pubsub client")
	}

	return c, nil
}

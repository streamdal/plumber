package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in GCP PubSub.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	defer client.Close()

	g := &GCPPubSub{
		Options: opts,
		msgDesc: md,
		client:  client,
		log:     logrus.WithField("pkg", "gcppubsub/write.go"),
	}

	ctx := context.Background()

	for _, value := range writeValues {
		if err := g.Write(ctx, value); err != nil {
			g.log.Error(err)
		}
	}

	return nil
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (g *GCPPubSub) Write(ctx context.Context, value []byte) error {
	t := g.client.Topic(g.Options.GCPPubSub.WriteTopicId)

	result := t.Publish(ctx, &pubsub.Message{
		Data: value,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	_, err := result.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to ensure that message was published")
	}

	return nil
}

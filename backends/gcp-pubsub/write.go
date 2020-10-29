package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in GCP PubSub.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	msg, err := writer.GenerateWriteValue(opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	g := &GCPPubSub{
		Options: opts,
		Client:  client,
		log:     logrus.WithField("pkg", "gcppubsub/read.go"),
	}

	return g.Write(context.Background(), msg)
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (g *GCPPubSub) Write(ctx context.Context, value []byte) error {
	t := g.Client.Topic(g.Options.GCPPubSub.WriteTopicId)

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

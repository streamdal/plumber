package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/dynamic"
)

func (g *GCPPubSub) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.New("unable to validate write options")
	}

	// Start up dynamic connection
	grpc, err := dynamic.New(dynamicOpts, "GCP PUbSub")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	t := g.client.Topic(dynamicOpts.GcpPubsub.Args.TopicId)

	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			result := t.Publish(ctx, &pubsub.Message{
				Data: outbound.Blob,
			})

			if _, err := result.Get(ctx); err != nil {
				g.log.Errorf("Unable to replay message: %s", err)
				continue
			}

			g.log.Debugf("Replayed message to GCP Pubsub topic '%s' for replay '%s'", dynamicOpts.GcpPubsub.Args.TopicId, outbound.ReplayId)

		case <-ctx.Done():
			g.log.Warning("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return errors.New("dynamic options cannot be nil")
	}

	if dynamicOpts.GcpPubsub == nil {
		return errors.New("backend group options cannot be nil")
	}

	if dynamicOpts.GcpPubsub.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if dynamicOpts.GcpPubsub.Args.TopicId == "" {
		return errors.New("Topic ID cannot be empty")
	}

	return nil
}

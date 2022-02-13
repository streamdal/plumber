package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (g *GCPPubSub) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.New("unable to validate write options")
	}

	if err := dynamicSvc.Start(ctx, "GCP PubSub", errorCh); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	t := g.client.Topic(dynamicOpts.GcpPubsub.Args.TopicId)

	outboundCh := dynamicSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			result := t.Publish(ctx, &pubsub.Message{
				Data: outbound.Blob,
			})

			if _, err := result.Get(ctx); err != nil {
				g.log.Errorf("Unable to replay message: %s", err)
				continue
			}

			g.log.Debugf("Replayed message to GCP Pubsub topic '%s' for replay '%s'", dynamicOpts.GcpPubsub.Args.TopicId, outbound.ReplayId)

		case <-ctx.Done():
			g.log.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.GcpPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.GcpPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.GcpPubsub.Args.TopicId == "" {
		return errors.New("Topic ID cannot be empty")
	}

	return nil
}

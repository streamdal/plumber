package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (g *GCPPubSub) Tunnel(ctx context.Context, tunnelOpts *opts.DynamicOptions, dynamicSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.New("unable to validate write options")
	}

	if err := dynamicSvc.Start(ctx, "GCP PubSub", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	t := g.client.Topic(tunnelOpts.GcpPubsub.Args.TopicId)

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

			g.log.Debugf("Replayed message to GCP Pubsub topic '%s' for replay '%s'", tunnelOpts.GcpPubsub.Args.TopicId, outbound.ReplayId)

		case <-ctx.Done():
			g.log.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.DynamicOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if tunnelOpts.GcpPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.GcpPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.GcpPubsub.Args.TopicId == "" {
		return errors.New("Topic ID cannot be empty")
	}

	return nil
}

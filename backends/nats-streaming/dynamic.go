package nats_streaming

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/dynamic"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"
)

func (n *NatsStreaming) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := n.log.WithField("pkg", "nats-streaming/dynamic")

	go dynamicSvc.Start("Nats Streaming")

	outboundCh := dynamicSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			if err := n.stanClient.Publish(dynamicOpts.NatsStreaming.Args.Channel, outbound.Blob); err != nil {
				err = fmt.Errorf("unable to replay message: %s", err)
				llog.Error(err)
				return err
			}

			llog.Debugf("Replayed message to NATS streaming channel '%s' for replay '%s'",
				dynamicOpts.NatsStreaming.Args.Channel, outbound.ReplayId)
		case <-ctx.Done():
			llog.Warning("context cancelled")
			return nil
		}
	}
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.NatsStreaming == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.NatsStreaming.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.NatsStreaming.Args.Channel == "" {
		return ErrEmptyChannel
	}

	return nil
}

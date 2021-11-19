package nats_streaming

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dynamic"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"
)

func (n *NatsStreaming) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := logrus.WithField("pkg", "nats-streaming/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(dynamicOpts, "Nats Streaming")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := n.stanClient.Publish(dynamicOpts.NatsStreaming.Args.Channel, outbound.Blob); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to NATS streaming channel '%s' for replay '%s'",
				dynamicOpts.NatsStreaming.Args.Channel, outbound.ReplayId)
		}
	}

	return nil
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
		return ErrMissingChannel
	}

	return nil
}

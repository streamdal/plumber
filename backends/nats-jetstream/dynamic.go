package nats_jetstream

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/dynamic"
)

func (n *NatsJetstream) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := n.log.WithField("pkg", "nats-jetstream/dynamic")

	if err := dynamicSvc.Start(ctx, "Nats", errorCh); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	stream := dynamicOpts.NatsJetstream.Args.Stream

	outboundCh := dynamicSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			if err := n.client.Publish(stream, outbound.Blob); err != nil {
				n.log.Errorf("Unable to replay message: %s", err)
				break
			}

			n.log.Debugf("Replayed message to Nats stream '%s' for replay '%s'", stream, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.NatsJetstream == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.NatsJetstream.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.NatsJetstream.Args.Stream == "" {
		return ErrMissingStream
	}

	return nil
}

package nats

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (n *Nats) Tunnel(ctx context.Context, tunnelOpts *opts.DynamicOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	llog := n.log.WithField("pkg", "nats/tunnel")

	if err := tunnelSvc.Start(ctx, "Nats", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	subject := tunnelOpts.Nats.Args.Subject

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			if err := n.Client.Publish(subject, outbound.Blob); err != nil {
				n.log.Errorf("Unable to replay message: %s", err)
				break
			}

			n.log.Debugf("Replayed message to Nats topic '%s' for replay '%s'", subject, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.DynamicOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if tunnelOpts.Nats == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.Nats.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.Nats.Args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}

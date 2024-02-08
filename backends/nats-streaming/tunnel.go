package nats_streaming

import (
	"context"
	"fmt"

	"github.com/streamdal/plumber/tunnel"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/validate"
)

func (n *NatsStreaming) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := n.log.WithField("pkg", "nats-streaming/tunnel")

	if err := tunnelSvc.Start(ctx, "Nats Streaming", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			if err := n.stanClient.Publish(tunnelOpts.NatsStreaming.Args.Channel, outbound.Blob); err != nil {
				err = fmt.Errorf("unable to replay message: %s", err)
				llog.Error(err)
				return err
			}

			llog.Debugf("Replayed message to NATS streaming channel '%s' for replay '%s'",
				tunnelOpts.NatsStreaming.Args.Channel, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.NatsStreaming == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.NatsStreaming.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.NatsStreaming.Args.Channel == "" {
		return ErrEmptyChannel
	}

	return nil
}

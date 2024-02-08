package rpubsub

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/tunnel"
	"github.com/streamdal/plumber/validate"
)

func (r *RedisPubsub) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOpts(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := logrus.WithField("pkg", "rpubsub/tunnel")

	if err := tunnelSvc.Start(ctx, "Redis PubSub", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			for _, ch := range tunnelOpts.RedisPubsub.Args.Channels {
				err := r.client.Publish(context.Background(), ch, outbound.Blob).Err()
				if err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}

				llog.Debugf("Replayed message to Redis PubSub channel '%s' for replay '%s'", ch, outbound.ReplayId)
			}
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}

	}
}

func validateTunnelOpts(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.RedisPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.RedisPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(tunnelOpts.RedisPubsub.Args.Channels) == 0 {
		return ErrMissingChannel
	}

	return nil
}

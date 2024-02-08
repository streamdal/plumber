package activemq

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/tunnel"
	"github.com/streamdal/plumber/validate"
)

func (a *ActiveMQ) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := a.log.WithField("pkg", "activemq/tunnel")

	if err := tunnelSvc.Start(ctx, "ActiveMQ", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	destination := getDestinationWrite(tunnelOpts.Activemq.Args)

	outboundCh := tunnelSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			if err := a.client.Send(destination, "", outbound.Blob, nil); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to ActiveMQ '%s' for replay '%s'", destination, outbound.ReplayId)
		case <-ctx.Done():
			a.log.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.Activemq == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := tunnelOpts.Activemq.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrTopicOrQueue
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrTopicAndQueue
	}

	return nil
}

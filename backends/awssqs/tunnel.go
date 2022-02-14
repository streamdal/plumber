package awssqs

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (a *AWSSQS) Tunnel(ctx context.Context, opts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	llog := a.log.WithField("pkg", "awssqs/tunnel")

	args := opts.AwsSqs.Args

	queueURL, err := a.getQueueURL(args.QueueName, args.RemoteAccountId)
	if err != nil {
		return errors.Wrap(err, "unable to get queue url")
	}

	if err := tunnelSvc.Start(ctx, "AWS SQS", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			// write
			if err := a.writeMsg(args, string(outbound.Blob), queueURL); err != nil {
				err = fmt.Errorf("unable to replay message: %s", err)
				llog.Error(err)
				return err
			}
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.AwsSqs == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.AwsSqs.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.AwsSqs.Args.QueueName == "" {
		return ErrMissingQueue
	}

	return nil
}

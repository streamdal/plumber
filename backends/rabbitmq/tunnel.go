package rabbitmq

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitMQ) Tunnel(ctx context.Context, opts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	llog := r.log.WithField("pkg", "rabbitmq/tunnel")

	// Nil check so that this can be injected into the struct for testing
	if r.client == nil {
		producer, err := r.newRabbitForWrite(opts.Rabbit.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create rabbitmq producer")
		}

		r.client = producer
	}

	defer r.client.Close()

	if err := tunnelSvc.Start(ctx, "RabbitMQ", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			if err := r.client.Publish(ctx, opts.Rabbit.Args.RoutingKey, outbound.Blob); err != nil {
				err = errors.Wrap(err, "Unable to reply message")
				llog.Error(err)
				return err
			}

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

	if tunnelOpts.Rabbit == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := tunnelOpts.Rabbit.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.RoutingKey == "" {
		return ErrEmptyRoutingKey
	}

	if args.ExchangeName == "" {
		return ErrEmptyExchangeName
	}

	return nil
}

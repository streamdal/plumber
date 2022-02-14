package rabbitmq

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitMQ) Dynamic(ctx context.Context, opts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	llog := r.log.WithField("pkg", "rabbitmq/dynamic")

	// Nil check so that this can be injected into the struct for testing
	if r.client == nil {
		producer, err := r.newRabbitForWrite(opts.Rabbit.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create rabbitmq producer")
		}

		r.client = producer
	}

	if err := dynamicSvc.Start(ctx, "RabbitMQ", errorCh); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	outboundCh := dynamicSvc.Read()

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

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Rabbit == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := dynamicOpts.Rabbit.Args
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

package rabbitmq

import (
	"context"

	"github.com/batchcorp/plumber/validate"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/dynamic"
)

func (r *RabbitMQ) Dynamic(ctx context.Context, opts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic) error {
	llog := logrus.WithField("pkg", "rabbitmq/dynamic")

	if err := validateDynamicOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	producer, err := r.newRabbitForWrite(opts.Rabbit.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq producer")
	}

	defer producer.Close()

	go dynamicSvc.Start("RabbitMQ")

	outboundCh := dynamicSvc.Read()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-outboundCh:
			if err := producer.Publish(ctx, opts.Rabbit.Args.RoutingKey, outbound.Blob); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break MAIN
			}

		case <-ctx.Done():
			r.log.Warning("context cancelled")
			break MAIN
		}
	}

	r.log.Debug("dynamic exiting")

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Rabbit == nil {
		return errors.New("rabbit options cannot be nil")
	}

	if dynamicOpts.Rabbit.Args == nil {
		return errors.New("rabbit args cannot be nil")
	}

	return nil
}

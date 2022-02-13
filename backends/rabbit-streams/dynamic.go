package rabbit_streams

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitStreams) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := r.log.WithField("pkg", "rabbit-streams/dynamic")

	// Make available to handleErr
	r.streamName = dynamicOpts.RabbitStreams.Args.Stream

	producer, err := r.client.NewProducer(dynamicOpts.RabbitStreams.Args.Stream,
		stream.NewProducerOptions().
			SetProducerName(dynamicOpts.RedisStreams.Args.WriteId).
			SetBatchSize(1))

	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq streams producer")
	}

	defer producer.Close()

	if err := dynamicSvc.Start(ctx, "RabbitMQ Streams"); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	outboundCh := dynamicSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			if err := producer.Send(amqp.NewMessage(outbound.Blob)); err != nil {
				err = fmt.Errorf("unable to replay message: %s", err)
				llog.Error(err)
				return err
			}

			llog.Debugf("Replayed message to Rabbit stream '%s' for replay '%s'",
				dynamicOpts.RabbitStreams.Args.Stream, outbound.ReplayId)

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

	if dynamicOpts.RabbitStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.RabbitStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.RabbitStreams.Args.Stream == "" {
		return ErrEmptyStream
	}

	return nil
}

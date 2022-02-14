package rabbit_streams

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitStreams) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := r.log.WithField("pkg", "rabbit-streams/tunnel")

	// Make available to handleErr
	r.streamName = tunnelOpts.RabbitStreams.Args.Stream

	producer, err := r.client.NewProducer(tunnelOpts.RabbitStreams.Args.Stream,
		stream.NewProducerOptions().
			SetProducerName(tunnelOpts.RedisStreams.Args.WriteId).
			SetBatchSize(1))

	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq streams producer")
	}

	defer producer.Close()

	if err := tunnelSvc.Start(ctx, "RabbitMQ Streams", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

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
				tunnelOpts.RabbitStreams.Args.Stream, outbound.ReplayId)

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

	if tunnelOpts.RabbitStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.RabbitStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.RabbitStreams.Args.Stream == "" {
		return ErrEmptyStream
	}

	return nil
}

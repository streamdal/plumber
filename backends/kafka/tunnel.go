package kafka

import (
	"context"

	"github.com/batchcorp/plumber/validate"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tunnel"
)

// Tunnels starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (k *Kafka) Tunnel(ctx context.Context, opts *opts.DynamicOptions, dynamicSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	llog := logrus.WithField("pkg", "kafka/tunnel")

	if err := validateTunnelOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	// Start up writer
	writer, err := NewWriter(k.dialer, k.connArgs)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Close()

	if err := dynamicSvc.Start(ctx, "Kafka", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := dynamicSvc.Read()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-outboundCh:
			for _, topic := range opts.Kafka.Args.Topics {
				if err := writer.WriteMessages(ctx, skafka.Message{
					Topic: topic,
					Key:   []byte(opts.Kafka.Args.Key),
					Value: outbound.Blob,
				}); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break MAIN
				}
			}

		case <-ctx.Done():
			k.log.Debug("context cancelled")
			break MAIN
		}
	}

	k.log.Debug("tunnel exiting")

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.DynamicOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if tunnelOpts.Kafka == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.Kafka.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(tunnelOpts.Kafka.Args.Topics) == 0 {
		return ErrMissingTopic
	}

	return nil
}

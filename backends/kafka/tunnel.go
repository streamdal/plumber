package kafka

import (
	"context"
	"encoding/base64"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

// Tunnels starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (k *Kafka) Tunnel(ctx context.Context, opts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
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

	if err := tunnelSvc.Start(ctx, "Kafka", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-outboundCh:
			headers := make([]kafka.Header, 0)

			if len(outbound.Metadata) > 0 {
				headers = k.generateKafkaHeaders(outbound)
			}

			for _, topic := range opts.Kafka.Args.Topics {
				if err := writer.WriteMessages(ctx, skafka.Message{
					Topic:   topic,
					Key:     []byte(opts.Kafka.Args.Key),
					Value:   outbound.Blob,
					Headers: headers,
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

func (k *Kafka) generateKafkaHeaders(o *events.Outbound) []kafka.Header {
	headers := make([]kafka.Header, 0)

	for mdKey, mdVal := range o.Metadata {
		var value []byte
		var err error
		if util.IsBase64(mdVal) {
			value, err = base64.StdEncoding.DecodeString(mdVal)
			if err != nil {
				k.log.Errorf("Unable to decode header '%s' with value '%s' for replay '%s'", mdKey, mdVal, o.ReplayId)
				continue
			}
		} else {
			value = []byte(mdVal)
		}

		headers = append(headers, kafka.Header{Key: mdKey, Value: value})
	}

	return headers
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
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

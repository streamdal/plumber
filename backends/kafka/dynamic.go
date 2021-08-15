package kafka

import (
	"context"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (k *Kafka) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "kafka/dynamic")

	// Start up writer
	writer, err := NewWriter(k.dialer, k.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(k.Options, "Kafka")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			for _, topic := range k.Options.Kafka.Topics {
				if err := writer.WriteMessages(ctx, skafka.Message{
					Topic: topic,
					Key:   []byte(k.Options.Kafka.WriteKey),
					Value: outbound.Blob,
				}); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break MAIN
				}
			}

		case <-ctx.Done():
			k.log.Warning("context cancelled")
			break MAIN
		}
	}

	k.log.Debug("dynamic exiting")

	return nil
}

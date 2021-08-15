package kafka

import (
	"context"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (k *Kafka) Dynamic(ctx context.Context) error {
	ctx := context.Background()
	llog := logrus.WithField("pkg", "kafka/dynamic")

	// Start up writer
	writer, err := NewWriter(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Conn.Close()
	defer writer.Writer.Close()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "Kafka")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := writer.Writer.WriteMessages(ctx, skafka.Message{
				Key:   []byte(opts.Kafka.WriteKey),
				Value: outbound.Blob,
			}); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Kafka topic '%s' for replay '%s'", opts.Kafka.Topics[0], outbound.ReplayId)
		}
	}
}

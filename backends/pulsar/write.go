package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

func (p *Pulsar) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.Options.Pulsar.Topic,
	})
	if err != nil {
		return errors.Wrap(err, "unable to create Pulsar producer")
	}

	defer producer.Close()

	for _, msg := range messages {
		if err := p.write(ctx, msg.Value); err != nil {
			util.WriteError(p.log, errorCh, err)
		}
	}

	return nil
}

// Write writes a message to an ActiveMQ topic
func (p *Pulsar) write(ctx context.Context, value []byte) error {
	_, err := p.producer.Send(ctx, &pulsar.ProducerMessage{Payload: value})
	if err != nil {
		p.log.Infof("Unable to write message to topic '%s': %s", p.Options.Pulsar.Topic, err)
		return errors.Wrap(err, "unable to write message")
	}

	p.log.Infof("Successfully wrote message to topic '%s'", p.Options.Pulsar.Topic)

	return nil
}

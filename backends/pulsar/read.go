package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
)

func (p *Pulsar) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(p.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	p.log.Info("Listening for message(s) ...")

	consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            p.Options.Pulsar.Topic,
		SubscriptionName: p.Options.Pulsar.SubscriptionName,
		Type:             p.getSubscriptionType(),
	})
	if err != nil {
		return errors.Wrap(err, "unable to create consumer")
	}

	defer consumer.Close()
	defer consumer.Unsubscribe()

	count := 1

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			return errors.Wrap(err, "unable to receive on consumer")
		}

		resultsChan <- &types.ReadMessage{
			Value: msg.Payload(),
			Metadata: map[string]interface{}{
				"id":               msg.ID(),
				"key":              msg.Key(),
				"topic":            msg.Topic(),
				"properties":       msg.Properties(),
				"redelivery_count": msg.RedeliveryCount(),
				"event_time":       msg.EventTime(),
				"is_replicated":    msg.IsReplicated(),
				"ordering_key":     msg.OrderingKey(),
				"producer_name":    msg.ProducerName(),
				"publish_time":     msg.PublishTime(),
			},
			ReceivedAt: time.Now().UTC(),
			Num:        count,
		}

		consumer.Ack(msg)

		if !p.Options.Read.Follow {
			break
		}
	}

	p.log.Debug("reader exiting")

	return nil
}

// validateReadOptions ensures all specified read flags are correct
// TODO: Fill this out
func validateReadOptions(opts *options.Options) error {
	return nil
}

// getSubscriptionType converts string input of the subscription type to pulsar library's equivalent
func (p *Pulsar) getSubscriptionType() pulsar.SubscriptionType {
	switch p.Options.Pulsar.SubscriptionType {
	case "exclusive":
		return pulsar.Exclusive
	case "failover":
		return pulsar.Failover
	case "keyshared":
		return pulsar.KeyShared
	default:
		return pulsar.Shared
	}
}

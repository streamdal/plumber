package rabbitmq_streams

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/batchcorp/plumber/types"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func (r *RabbitMQStreams) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	client, err := newClient(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new client")
	}

	defer client.Close()

	var count int

	offsetOption, err := r.getOffsetOption()
	if err != nil {
		return errors.Wrap(err, "could not read messages")
	}

	handleMessage := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		for _, value := range message.Data {
			count++

			resultsChan <- &types.ReadMessage{
				Value: value,
				Metadata: map[string]interface{}{
					"delivery_tag":         message.DeliveryTag,
					"properties":           *message.Properties,
					"format":               message.Format,
					"header":               *message.Header,
					"delivery_annotations": message.DeliveryAnnotations,
					"footer":               message.Footer,
					"send_settled":         message.SendSettled,
					"consumer_context":     consumerContext,
				},
				ReceivedAt: time.Now().UTC(),
				Num:        count,
				Raw:        message,
			}
		}

		if !r.Options.Read.Follow {
			consumerContext.Consumer.Close()
		}
	}

	consumer, err := client.NewConsumer(r.Options.RabbitMQStreams.Stream,
		handleMessage,
		stream.NewConsumerOptions().
			SetConsumerName(r.Options.RabbitMQStreams.ClientName).
			SetOffset(offsetOption))
	if err != nil {
		return errors.Wrap(err, "unable to start rabbitmq streams consumer")
	}

	r.log.Infof("Waiting for messages on stream '%s'...", r.Options.RabbitMQStreams.Stream)

	closeCh := consumer.NotifyClose()

	select {
	case closeEvent := <-closeCh:
		// TODO: implement reconnect logic
		r.log.Debugf("Stream closed by remote host: %s", closeEvent.Reason)
	}

	return nil
}

func (r *RabbitMQStreams) getOffsetOption() (stream.OffsetSpecification, error) {
	offset := r.Options.RabbitMQStreams.Offset

	switch offset {
	case "last":
		return stream.OffsetSpecification{}.Last(), nil
	case "last-consumed":
		return stream.OffsetSpecification{}.LastConsumed(), nil
	case "first":
		return stream.OffsetSpecification{}.First(), nil
	case "next":
		return stream.OffsetSpecification{}.Next(), nil
	default:
		if v, err := strconv.ParseInt(offset, 10, 64); err == nil {
			return stream.OffsetSpecification{}.Offset(v), nil
		} else {
			return stream.OffsetSpecification{}.Next(),
				fmt.Errorf("unknown --offset value '%s'", offset)
		}
	}
}

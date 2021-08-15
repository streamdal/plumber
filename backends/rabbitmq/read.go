package rabbitmq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/rabbit"
)

// Read is the entry point function for performing read operations in RabbitMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func (r *RabbitMQ) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(r.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := newConnection(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new consumer")
	}

	defer client.Close()

	r.log.Info("Listening for message(s) ...")

	errCh := make(chan *rabbit.ConsumeError)
	ctxWithCancel, cancel := context.WithCancel(ctx)

	count := 1

	go client.Consume(ctxWithCancel, errCh, func(msg amqp.Delivery) error {
		resultsChan <- &types.ReadMessage{
			Value: msg.Body,
			Metadata: map[string]interface{}{
				"headers":          msg.Headers,
				"content_type":     msg.ContentType,
				"content_encoding": msg.ContentEncoding,
				"delivery_mode":    msg.DeliveryMode,
				"priority":         msg.Priority,
				"correlation_id":   msg.CorrelationId,
				"reply_to":         msg.ReplyTo,
				"expiration":       msg.Expiration,
				"message_id":       msg.MessageId,
				"timestamp":        msg.Timestamp,
				"type":             msg.Type,
				"user_id":          msg.UserId,
				"app_id":           msg.AppId,
				"consumer_tag":     msg.ConsumerTag,
				"message_count":    msg.MessageCount,
				"delivery_tag":     msg.DeliveryTag,
				"redelivered":      msg.Redelivered,
				"exchange":         msg.Exchange,
				"routing_key":      msg.RoutingKey,
			},
			ReceivedAt: time.Now().UTC(),
			Num:        count,
		}

		count++

		if !r.Options.Read.Follow {
			cancel()
		}

		return nil
	})

MAIN:
	for {
		select {
		case err := <-errCh:
			util.WriteError(r.log, errorChan, err.Error)
		case <-ctxWithCancel.Done():
			r.log.Debug("reader exiting")
			break MAIN
		}
	}

	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.Rabbit.Address == "" {
		return errors.New("--address cannot be empty")
	}

	return nil
}

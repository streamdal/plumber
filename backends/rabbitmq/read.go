package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/validate"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/rabbit"
)

func (r *RabbitMQ) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read config")
	}

	// Check if nil to allow unit testing injection into struct
	if r.client == nil {
		consumer, err := r.newRabbitForRead(readOpts.Rabbit.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create new rabbit consumer")
		}
		r.client = consumer
	}

	errCh := make(chan *rabbit.ConsumeError)
	ctx, cancel := context.WithCancel(context.Background())

	var count int64

	r.log.Info("Listening for messages...")

	go r.client.Consume(ctx, errCh, func(msg amqp.Delivery) error {
		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			return errors.Wrap(err, "unable to serialize RabbitMQ msg to JSON")
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			Metadata:            nil,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Body,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_Rabbit{
				Rabbit: &records.Rabbit{
					Body:            msg.Body,
					Timestamp:       msg.Timestamp.UTC().Unix(),
					Type:            msg.Type,
					Exchange:        msg.Exchange,
					RoutingKey:      msg.RoutingKey,
					ContentType:     msg.ContentType,
					ContentEncoding: msg.ContentEncoding,
					Priority:        int32(msg.Priority),
					Expiration:      msg.Expiration,
					MessageId:       msg.MessageId,
					UserId:          msg.UserId,
					AppId:           msg.AppId,
					ReplyTo:         msg.ReplyTo,
					CorrelationId:   msg.CorrelationId,
					Headers:         convertAMQPHeadersToProto(msg.Headers),
				},
			},
		}

		if !readOpts.Continuous {
			cancel()
		}

		return nil
	})

	for {
		select {
		case err := <-errCh:
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               err.Error.Error(),
			}
		case <-ctx.Done():
			r.log.Debug("Reader exiting")
			return nil
		}
	}
}

func convertAMQPHeadersToProto(table amqp.Table) []*records.RabbitHeader {
	headers := make([]*records.RabbitHeader, 0)
	for k, v := range table {
		headers = append(headers, &records.RabbitHeader{
			Key:   k,
			Value: fmt.Sprintf("%s", v),
		})
	}

	return headers
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.Rabbit == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.Rabbit.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.ExchangeName == "" {
		return ErrEmptyExchangeName
	}

	if args.QueueName == "" {
		return ErrEmptyQueueName
	}

	if args.BindingKey == "" {
		return ErrEmptyBindingKey
	}

	return nil
}

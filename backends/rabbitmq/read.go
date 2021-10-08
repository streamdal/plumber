package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

	consumer, err := r.newRabbitForRead(readOpts.Rabbit.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create new rabbit consumer")
	}

	defer consumer.Close()

	errCh := make(chan *rabbit.ConsumeError)
	ctx, cancel := context.WithCancel(context.Background())

	var count int64

	r.log.Info("Listening for messages...")

	go consumer.Consume(ctx, errCh, func(msg amqp.Delivery) error {
		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			return errors.Wrap(err, "unable to serialize kafka msg to JSON")
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

	return nil
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
	if readOpts.Rabbit == nil {
		return errors.New("rabbit read options cannot be nil")
	}

	if readOpts.Rabbit.Args == nil {
		return errors.New("rabbit read option args cannot be nil")
	}

	return nil
}

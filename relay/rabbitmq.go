package relay

import (
	"context"
	"time"

	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/rabbitmq/types"
)

func (r *Relay) handleRabbit(ctx context.Context, conn *grpc.ClientConn, msg *types.RelayMessage) error {
	if err := r.validateRabbitRelayMessage(msg); err != nil {
		return errors.Wrap(err, "unable to validate RabbitMQ relay message")
	}

	rabbitRecord := convertRabbitMessageToProtobufRecord(msg.Value)

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddAMQPRecord(ctx, &services.AMQPRecordRequest{
		Records: []*records.AMQPSinkRecord{rabbitRecord},
	}); err != nil {
		return errors.Wrap(err, "unable to complete AddAMQPRecord call")
	}

	return nil
}

func (r *Relay) validateRabbitRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.Value == nil {
		return errors.New("msg.Value cannot be nil")
	}

	return nil
}

func convertRabbitMessageToProtobufRecord(msg *amqp.Delivery) *records.AMQPSinkRecord {
	return &records.AMQPSinkRecord{
		Body:            msg.Body,
		Timestamp:       time.Now().UTC().UnixNano(),
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
	}
}

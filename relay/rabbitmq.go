package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/rabbitmq/types"
)

func (r *Relay) handleRabbit(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	var err error

	sinkRecords, err := r.convertMessagesToAMQPSinkRecords(messages)
	if err != nil {
		return errors.Wrap(err, "unable to convert messages to rabbit sink records")
	}

	client := services.NewGRPCCollectorClient(conn)

	return r.CallWithRetry(ctx, "AddAMQPRecord", func(ctx context.Context) error {
		_, err := client.AddAMQPRecord(ctx, &services.AMQPRecordRequest{
			Records: sinkRecords,
		}, grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (r *Relay) validateAMQPRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.Value == nil {
		return errors.New("msg.Value cannot be nil")
	}

	return nil
}

func (r *Relay) convertMessagesToAMQPSinkRecords(messages []interface{}) ([]*records.AMQPSinkRecord, error) {
	sinkRecords := make([]*records.AMQPSinkRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateAMQPRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate kafka relay message (index: %d): %s", i, err)
		}

		sinkRecords = append(sinkRecords, &records.AMQPSinkRecord{
			Body:            relayMessage.Value.Body,
			Timestamp:       time.Now().UTC().UnixNano(),
			Type:            relayMessage.Value.Type,
			Exchange:        relayMessage.Value.Exchange,
			RoutingKey:      relayMessage.Value.RoutingKey,
			ContentType:     relayMessage.Value.ContentType,
			ContentEncoding: relayMessage.Value.ContentEncoding,
			Priority:        int32(relayMessage.Value.Priority),
			Expiration:      relayMessage.Value.Expiration,
			MessageId:       relayMessage.Value.MessageId,
			UserId:          relayMessage.Value.UserId,
			AppId:           relayMessage.Value.AppId,
			ReplyTo:         relayMessage.Value.ReplyTo,
			CorrelationId:   relayMessage.Value.CorrelationId,
		})
	}

	return sinkRecords, nil
}

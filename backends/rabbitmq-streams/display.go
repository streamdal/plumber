package rabbitmq_streams

import (
	"fmt"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func (r *RabbitMQStreams) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*amqp.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(r.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := make([][]string, 0)

	consumerCtxRaw, ok := msg.Metadata["consumer_context"]
	if ok {
		consumerCtx, ok := consumerCtxRaw.(stream.ConsumerContext)
		if ok {
			properties = [][]string{
				{"consumer Name", consumerCtx.Consumer.GetName()},
				{"Stream", consumerCtx.Consumer.GetStreamName()},
				{"Offset", fmt.Sprintf("%d", consumerCtx.Consumer.GetOffset())},
			}
		}
	}

	if rawMsg.Properties != nil {
		properties = append(properties, []string{
			"Message ID",
			fmt.Sprintf("%s", rawMsg.Properties.MessageID),
		})

		properties = append(properties, []string{
			"Creation Time",
			rawMsg.Properties.CreationTime.String(),
		})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (r *RabbitMQStreams) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

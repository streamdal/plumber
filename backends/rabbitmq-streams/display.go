package rabbitmq_streams

import (
	"fmt"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
)

func (r *RabbitMQStreams) DisplayMessage(msg *types.ReadMessage) error {
	properties := [][]string{
		{"consumer Name", ctx.Consumer.GetName()},
		{"Stream", ctx.Consumer.GetStreamName()},
		{"Offset", fmt.Sprintf("%d", ctx.Consumer.GetOffset())},
	}

	ts := time.Now().UTC()

	if msg.Properties != nil {
		ts = msg.Properties.CreationTime
		properties = append(properties, []string{
			"Message ID",
			fmt.Sprintf("%s", msg.Properties.MessageID),
		})
	}

	printer.PrintTable(properties, count, ts, data)
}

func (r *RabbitMQStreams) DisplayError(msg *types.ErrorMessage) error {
	return nil
}

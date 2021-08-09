package printer

import (
	"fmt"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

	"github.com/batchcorp/plumber/options"
)

func PrintRabbitMQStreamsResult(opts *options.Options, count int, ctx stream.ConsumerContext, msg *amqp.Message, data []byte) {
	properties := [][]string{
		{"Consumer Name", ctx.Consumer.GetName()},
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

	printTable(properties, count, ts, data)
}

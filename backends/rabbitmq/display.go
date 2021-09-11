package rabbitmq

import (
	"fmt"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func (r *RabbitMQ) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(amqp.Delivery)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(r.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Message ID", rawMsg.MessageId},
		{"Exchange", rawMsg.Exchange},
		{"Routing Key", rawMsg.RoutingKey},
		{"Type", rawMsg.Type},
		{"App ID", rawMsg.AppId},
		{"Correlation ID", rawMsg.CorrelationId},
	}

	for k, v := range rawMsg.Headers {
		properties = append(properties, []string{
			k, fmt.Sprint(v),
		})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (r *RabbitMQ) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

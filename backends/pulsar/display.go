package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
)

func (p *Pulsar) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(pulsar.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(p.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Key", rawMsg.Key()},
		{"Topic", rawMsg.Topic()},
		{"Ordering Key", rawMsg.OrderingKey()},
		{"Producer Name", rawMsg.ProducerName()},
	}

	for k, v := range rawMsg.Properties() {
		properties = append(properties, []string{
			k, v,
		})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (p *Pulsar) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

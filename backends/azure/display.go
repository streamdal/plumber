package azure

import (
	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
)

func (s *ServiceBus) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*servicebus.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(s.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"ID", rawMsg.ID},
		{"Content Type", rawMsg.ContentType},
		{"To", rawMsg.To},
		{"Reply To", rawMsg.ReplyTo},
		{"Correlation ID", rawMsg.CorrelationID},
		{"Label", rawMsg.Label},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (s *ServiceBus) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

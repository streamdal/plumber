package azure_eventhub

import (
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
)

func (e *EventHub) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*eventhub.Event)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(e.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"ID", rawMsg.ID},
		{"Partition Key", derefString(rawMsg.PartitionKey)},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (e *EventHub) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func derefString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}

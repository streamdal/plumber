package activemq

import (
	"fmt"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp/v3"
	"github.com/pkg/errors"
)

func (a *ActiveMq) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*stomp.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(a.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Content Type", rawMsg.ContentType},
		{"Destination", rawMsg.Destination},
	}

	if rawMsg.Subscription != nil {
		properties = append(properties, []string{
			"Subscription ID", rawMsg.Subscription.Id(),
		}, []string{
			"Subscription Active", fmt.Sprint(rawMsg.Subscription.Active()),
		})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (a *ActiveMq) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

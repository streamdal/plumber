package nats

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
)

func (n *Nats) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*nats.Msg)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(n.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Subject", rawMsg.Subject},
		{"Reply", rawMsg.Reply},
	}

	if rawMsg.Sub != nil {
		properties = append(properties, []string{
			"Queue", rawMsg.Sub.Queue,
		})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (n *Nats) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

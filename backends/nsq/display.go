package nsq

import (
	"fmt"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
)

func (n *NSQ) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*nsq.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(n.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Message ID", fmt.Sprintf("%s", rawMsg.ID)},
		{"topic", n.Options.NSQ.Topic},
		{"OutputChannel", n.Options.NSQ.Channel},
		{"Attempts", fmt.Sprintf("%d", rawMsg.Attempts)},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (n *NSQ) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

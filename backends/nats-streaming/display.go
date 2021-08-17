package nats_streaming

import (
	"fmt"

	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
)

func (n *NatsStreaming) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*stan.Msg)
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
		{"Redelivered", fmt.Sprint(rawMsg.Redelivered)},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (n *NatsStreaming) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

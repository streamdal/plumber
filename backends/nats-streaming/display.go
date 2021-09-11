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
		{"Redelivered", fmt.Sprint(rawMsg.Redelivered)},
	}

	if n.Options.Read.Verbose {
		properties = append(properties,
			[]string{
				"Reply", rawMsg.Reply,
			},
			[]string{
				"CRC32", fmt.Sprint(rawMsg.CRC32),
			},
			[]string{
				"Timestamp", fmt.Sprint(rawMsg.Timestamp),
			},
			[]string{
				"Redelivery Count", fmt.Sprint(rawMsg.RedeliveryCount),
			})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (n *NatsStreaming) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

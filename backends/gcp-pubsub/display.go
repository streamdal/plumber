package gcppubsub

import (
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
)

func (g *GCPPubSub) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*pubsub.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(g.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"ID", rawMsg.ID},
		{"Ordering Key", rawMsg.OrderingKey},
		{"Delivery Attempt", fmt.Sprint(*rawMsg.DeliveryAttempt)},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (g *GCPPubSub) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

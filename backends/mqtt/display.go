package mqtt

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
)

func (m *MQTT) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(mqtt.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	// TODO: Remove
	m.log.Debugf("DisplayMessage received msg. Length: '%d'; Contents: %s\n", len(msg.Value), msg.Value)

	decoded, err := reader.Decode(m.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Topic", rawMsg.Topic()},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (m *MQTT) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

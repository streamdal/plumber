package mqtt

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (m *MQTT) DisplayMessage(msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetMqtt()
	if record == nil {
		return errors.New("BUG: record in message is nil")
	}

	properties := [][]string{
		{"ID", fmt.Sprintf("%d", record.Id)},
		{"Topic", record.Topic},
		{"QoS", fmt.Sprint(record.Qos)},
		{"Retain", fmt.Sprintf("%t", record.Duplicate)},
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(properties, msg.Num, receivedAt, msg.Payload)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (m *MQTT) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetMqtt() == nil {
		return errors.New("mqtt message cannot be nil")
	}

	return nil
}

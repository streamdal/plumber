package activemq

import (
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (a *ActiveMQ) DisplayMessage(msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetActivemq()

	properties := [][]string{
		{"Destination", record.Destination},
		{"Content Type", record.ContentType},
		{"Subscription ID", record.SubscriptionId},
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(properties, msg.Num, receivedAt, msg.Payload)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (a *ActiveMQ) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetActivemq() == nil {
		return errors.New("activemq message cannot be nil")
	}

	if msg.GetActivemq().Value == nil {
		return errors.New("message value cannot be nil")
	}

	return nil
}

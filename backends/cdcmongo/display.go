package cdcmongo

import (
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

func (m *Mongo) DisplayMessage(msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetMongo()
	if record == nil {
		return errors.New("BUG: record in message is nil")
	}

	properties := [][]string{{}}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(properties, msg.Num, receivedAt, msg.Payload)

	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (m *Mongo) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

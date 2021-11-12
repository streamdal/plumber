package cdcpostgres

import (
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

func (c *CDCPostgres) DisplayMessage(msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable([][]string{}, msg.Num, receivedAt, msg.Payload)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (c *CDCPostgres) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetPostgres() == nil {
		return errors.New("postgres message cannot be nil")
	}

	return nil
}

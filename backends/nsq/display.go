package nsq

import (
	"fmt"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

func (n *NSQ) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetNsq()

	properties := [][]string{
		{"Message ID", fmt.Sprintf("%s", record.Id)},
		{"Topic", record.Topic},
		{"Channel", record.Channel},
		{"Attempts", fmt.Sprintf("%d", record.Attempts)},
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (n *NSQ) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetNsq() == nil {
		return errors.New("NSQ message cannot be nil")
	}

	return nil
}

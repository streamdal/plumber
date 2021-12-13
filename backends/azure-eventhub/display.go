package azure_eventhub

import (
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (a *AzureEventHub) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetAzureEventHub()

	properties := [][]string{
		{"Message ID", record.Id},
	}

	for k, v := range record.SystemProperties {
		properties = append(properties, []string{k, v})
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (a *AzureEventHub) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetAzureEventHub() == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetAzureEventHub().Value == nil {
		return errors.New("message value cannot be nil")
	}

	return nil
}

package azure_servicebus

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (a *AzureServiceBus) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetAzureServiceBus()

	properties := [][]string{
		{"ID", record.Id},
		{"Label", record.Label},
		{"To", record.To},
		{"Delivery Count", fmt.Sprintf("%d", record.DeliveryCount)},
		{"Correlation ID", record.CorrelationId},
		{"Content Type", record.ContentType},
		{"Session ID", record.SessionId},
		{"Time To Live", time.Unix(record.Ttl, 0).Format(time.RFC3339)},
		{"Group Sequence", fmt.Sprintf("%d", record.GroupSequence)},
		{"Reply To", record.ReplyTo},
		{"Replay To Group ID", record.ReplyToGroupId},
		{"Lock Token", record.LockToken},
		{"Format", fmt.Sprintf("%d", record.Format)},
	}

	for k, v := range record.UserProperties {
		properties = append(properties, []string{k, v})
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (a *AzureServiceBus) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetAzureServiceBus() == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetAzureServiceBus().Value == nil {
		return errors.New("message value cannot be nil")
	}

	return nil
}

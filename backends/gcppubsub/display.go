package gcppubsub

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (g *GCPPubSub) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetGcpPubsub()
	if record == nil {
		return errors.New("BUG: record in message is nil")
	}

	properties := [][]string{
		{"ID", record.Id},
		{"Publish Time", time.Unix(record.PublishTime, 0).Format(time.RFC3339)},
		{"Delivery Attempt", fmt.Sprintf("%d", record.DeliveryAttempt)},
		{"Ordering Key", record.OrderingKey},
	}

	for k, v := range record.Attributes {
		properties = append(properties, []string{k, v})
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (g *GCPPubSub) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	return nil
}

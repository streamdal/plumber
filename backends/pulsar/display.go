package pulsar

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (p *Pulsar) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetPulsar()

	properties := [][]string{
		{"Message ID", record.Id},
		{"Key", record.Key},
		{"Topic", record.Topic},
		{"Redelivery Count", fmt.Sprintf("%d", record.RedeliveryCount)},
		{"Event Time", record.EventTime},
		{"Is Replicated", fmt.Sprintf("%t", record.IsReplicated)},
		{"Ordering Key", record.OrderingKey},
		{"Producer Name", record.ProducerName},
	}

	for k, v := range record.Properties {
		properties = append(properties, []string{k, v})
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (p *Pulsar) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetPulsar() == nil {
		return errors.New("message cannot be nil")
	}

	if msg.GetPulsar().Value == nil {
		return errors.New("message value cannot be nil")
	}

	return nil
}

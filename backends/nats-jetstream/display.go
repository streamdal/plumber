package nats_jetstream

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (n *NatsJetstream) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetNatsJetstream()
	if record == nil {
		return errors.New("BUG: record in message is nil")
	}

	properties := [][]string{
		{"Subject", record.Stream},
	}

	if cliOpts.Read.NatsJetstream.Args.CreateDurableConsumer || cliOpts.Read.NatsJetstream.Args.ExistingDurableConsumer {
		properties = append(properties, []string{"Consumer Name", record.ConsumerName})
		properties = append(properties, []string{"Stream Sequence", fmt.Sprint(record.Sequence)})
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (n *NatsJetstream) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetNatsJetstream() == nil {
		return errors.New("message value cannot be nil")
	}

	return nil
}

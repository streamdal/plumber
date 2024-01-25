package awskinesis

import (
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (k *Kinesis) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetAwsKinesis()

	properties := [][]string{
		{"Partition Key", record.PartitionKey},
		{"Sequence Number", record.SequenceNumber},
		{"Encryption Type", record.EncryptionType},
		{"Shard ID", record.ShardId},
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (k *Kinesis) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetAwsKinesis().Value == nil {
		return errors.New("message value cannot be nil")
	}

	return nil
}

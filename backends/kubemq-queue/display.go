package kubemq_queue

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

func (k *KubeMQ) DisplayMessage(msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetKubemq()

	properties := [][]string{
		{"Message ID", record.Id},
		{"Client ID", record.ClientId},
		{"Channel", record.Channel},
		{"Sequence", fmt.Sprintf("%d", record.Sequence)},
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(properties, msg.Num, receivedAt, msg.Payload)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (k *KubeMQ) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetKubemq() == nil {
		return errors.New("KubeMQ message cannot be nil")
	}

	return nil
}

package rabbit_streams

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/printer"
)

func (r *RabbitStreams) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetRabbitStreams()

	var properties [][]string

	if record.Header != nil {
		properties = [][]string{
			{"Tag", record.DeliveryTag},
			{"Format", fmt.Sprintf("%d", record.Format)},
			{"Priority", fmt.Sprintf("%d", record.Header.Priority)},
			{"Durable", fmt.Sprintf("%t", record.Header.Durable)},
			{"TTL", fmt.Sprintf("%d", record.Header.Ttl)},
			{"First Acquirer", fmt.Sprintf("%t", record.Header.FirstAcquirer)},
			{"Delivery Count", fmt.Sprintf("%d", record.Header.DeliveryCount)},
		}
	} else {
		properties = [][]string{
			{"Tag", record.DeliveryTag},
			{"Format", fmt.Sprintf("%d", record.Format)},
		}
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	for k, v := range record.DeliveryAnnotations {
		properties = append(properties, []string{k, v})
	}

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (r *RabbitStreams) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetRabbitStreams() == nil {
		return errors.New("rabbit streams message cannot be nil")
	}

	return nil
}

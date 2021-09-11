package kafka

import (
	"fmt"
	"strconv"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
)

func (k *Kafka) DisplayMessage(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	record := msg.GetKafka()
	if record == nil {
		return errors.New("BUG: record in message is nil")
	}

	key := aurora.Gray(12, "NONE").String()

	if len(record.Key) != 0 {
		key = string(record.Key)
	}

	properties := [][]string{
		{"Key", key},
		{"topic", record.Topic},
		{"Offset", fmt.Sprintf("%d", record.Offset)},
		{"Partition", fmt.Sprintf("%d", record.Partition)},
	}

	// Display offset info if it exists
	if lastOffset, ok := msg.Metadata["last_offset"]; ok {
		lastOffsetInt, err := strconv.ParseInt(lastOffset, 10, 64)

		if err != nil {
			k.log.Errorf("unable to parse last_offset '%s': %s", lastOffset, err)
		} else {
			lastOffStr := strconv.FormatUint(uint64(lastOffsetInt), 10)
			properties = append(properties, []string{"LastOffset", lastOffStr})
		}
	}

	properties = append(properties, generateHeaders(record.Headers)...)

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(properties, msg.Num, receivedAt, msg.Payload)

	return nil
}

func (k *Kafka) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func generateHeaders(headers []*records.KafkaHeader) [][]string {
	if len(headers) == 0 {
		return [][]string{
			[]string{"Header(s)", aurora.Gray(12, "NONE").String()},
		}
	}

	result := make([][]string, len(headers))
	result[0] = []string{
		"Header(s)", fmt.Sprintf("KEY: %s / VALUE: %s", headers[0].Key, string(headers[0].Value)),
	}

	for i := 1; i != len(headers); i++ {
		result[i] = []string{
			"", fmt.Sprintf("KEY: %s / VALUE: %s", headers[i].Key, string(headers[i].Value)),
		}
	}

	return result
}

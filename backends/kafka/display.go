package kafka

import (
	"fmt"
	"strconv"

	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

func (k *Kafka) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(kafka.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(k.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	key := aurora.Gray(12, "NONE").String()

	if len(rawMsg.Key) != 0 {
		key = string(rawMsg.Key)
	}

	properties := [][]string{
		{"Key", key},
		{"topic", rawMsg.Topic},
		{"Offset", fmt.Sprintf("%d", rawMsg.Offset)},
		{"Partition", fmt.Sprintf("%d", rawMsg.Partition)},
	}

	// Display offset info if it exists
	if lastOffset, ok := msg.Metadata["last_offset"]; ok {
		lastOffsetInt, ok := lastOffset.(int64)

		if ok {
			lastOffStr := strconv.FormatUint(uint64(lastOffsetInt), 10)
			properties = append(properties, []string{"LastOffset", lastOffStr})
		}
	}

	properties = append(properties, generateHeaders(rawMsg.Headers)...)

	printer.PrintTable(properties, msg.Num, rawMsg.Time, decoded)

	return nil
}

func (k *Kafka) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func generateHeaders(headers []kafka.Header) [][]string {
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

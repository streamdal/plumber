package printer

import (
	"fmt"
	"strconv"

	"github.com/logrusorgru/aurora"
	"github.com/segmentio/kafka-go"

	ktypes "github.com/batchcorp/plumber/backends/kafka/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

func PrintKafkaResult(opts *options.Options, msg *types.ReadMessage) error {
	key := aurora.Gray(12, "NONE").String()

	if len(msg.Key) != 0 {
		key = string(msg.Key)
	}

	properties := [][]string{
		{"Key", key},
		{"topic", msg.Topic},
		{"Offset", fmt.Sprintf("%d", msg.Offset)},
		{"Partition", fmt.Sprintf("%d", msg.Partition)},
	}

	if offsetInfo.LastOffset != 0 {

		lastOffStr := strconv.FormatUint(uint64(offsetInfo.LastOffset), 10)

		properties = append(properties, []string{"LastOfsset", lastOffStr})
	}

	properties = append(properties, generateHeaders(msg.Headers)...)

	printTable(properties, offsetInfo.Count, msg.Time, data)

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

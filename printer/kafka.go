package printer

import (
	"fmt"
	"strings"
	"time"

	"github.com/batchcorp/plumber/cli"
	"github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/kafka-go"
)

func PrintKafkaResult(opts *cli.Options, count int, msg kafka.Message, data []byte) {
	fmt.Printf("\n------------- [Count: %d Received at: %s] -------------------\n\n",
		aurora.Cyan(count), aurora.Yellow(msg.Time.Format(time.RFC3339)).String())

	key := aurora.Gray(12, "NONE").String()

	if len(msg.Key) != 0 {
		key = string(msg.Key)
	}

	properties := [][]string{
		[]string{"Key", key},
		[]string{"Topic", msg.Topic},
		[]string{"Offset", fmt.Sprintf("%d", msg.Offset)},
		[]string{"Partition", fmt.Sprintf("%d", msg.Partition)},
	}

	properties = append(properties, generateHeaders(msg.Headers)...)

	tableString := &strings.Builder{}

	table := tablewriter.NewWriter(tableString)
	table.AppendBulk(properties)
	table.SetColMinWidth(0, 20)
	table.SetColMinWidth(1, 40)
	// First column align left, second column align right
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_RIGHT})
	table.Render()

	fmt.Println(tableString.String())

	// Display value
	Print(string(data))
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

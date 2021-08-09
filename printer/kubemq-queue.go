package printer

import (
	"fmt"
	"time"

	"github.com/kubemq-io/kubemq-go/queues_stream"

	"github.com/batchcorp/plumber/cli"
)

func PrintKubeMQResult(_ *cli.Options, count int, msg *queues_stream.QueueMessage, data []byte) {
	properties := [][]string{
		{"Message ID", msg.MessageID},
		{"Client ID", msg.ClientID},
		{"Channel", msg.Channel},
		{"Sequence No.", fmt.Sprintf("%d", msg.Attributes.Sequence)},
	}

	ts := time.Unix(0, msg.Attributes.Timestamp)

	printTable(properties, count, ts, data)
}

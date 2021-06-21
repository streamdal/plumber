package printer

import (
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"

	"github.com/batchcorp/plumber/cli"
)

func PrintNSQResult(opts *cli.Options, count int, msg *nsq.Message, data []byte) {
	ts := time.Unix(0, msg.Timestamp)

	properties := [][]string{
		{"Message ID", fmt.Sprintf("%s", msg.ID)},
		{"Topic", opts.NSQ.Topic},
		{"Channel", opts.NSQ.Channel},
		{"Attempts", fmt.Sprintf("%d", msg.Attempts)},
	}

	printTable(properties, count, ts, data)
}

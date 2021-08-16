package nsq

import (
	"fmt"
	"time"

	"github.com/batchcorp/plumber/types"
)

func (n *NSQ) DisplayMessage(msg *types.ReadMessage) error {
	ts := time.Unix(0, msg.Timestamp)

	properties := [][]string{
		{"Message ID", fmt.Sprintf("%s", msg.ID)},
		{"topic", opts.NSQ.Topic},
		{"OutputChannel", opts.NSQ.Channel},
		{"Attempts", fmt.Sprintf("%d", msg.Attempts)},
	}

	printTable(properties, count, ts, data)

	return nil
}

func (n *NSQ) DisplayError(msg *types.ErrorMessage) error {
	return nil
}

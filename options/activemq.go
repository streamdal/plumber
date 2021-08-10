package options

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type ActiveMqOptions struct {
	// Shared
	Address  string
	Topic    string
	Queue    string
	ClientId string

	// Read
	ReadFollow bool
}

func HandleActiveMqFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("activemq", "ActiveMQ STOMP")
	addSharedActiveMqFlags(rc, opts)

	wc := writeCmd.Command("activemq", "ActiveMQ STOMP")
	addSharedActiveMqFlags(wc, opts)
}

func addSharedActiveMqFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("localhost:61613").StringVar(&opts.ActiveMq.Address)
	cmd.Flag("topic", "topic to read message(s) from").StringVar(&opts.ActiveMq.Topic)
	cmd.Flag("queue", "queue to read message(s) from").StringVar(&opts.ActiveMq.Queue)
}

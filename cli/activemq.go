package cli

import (
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

type ActiveMqOptions struct {
	// Shared
	Address  string
	Topic    string
	Queue    string
	Timeout  time.Duration
	ClientId string
	QoSLevel int

	// Read
	ReadFollow  bool
	ReadTimeout time.Duration

	// Write
	WriteTimeout time.Duration
}

func HandleActiveMqFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("activemq", "ActiveMq STOMP")

	addSharedActiveMqFlags(rc, opts)
	addReadActiveMqFlags(rc, opts)

	wc := writeCmd.Command("activemq", "ActiveMq STOMP")

	addSharedActiveMqFlags(wc, opts)
	addWriteActiveMqFlags(wc, opts)
}

func addSharedActiveMqFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("tcp://localhost:61613").StringVar(&opts.ActiveMq.Address)
	cmd.Flag("topic", "Topic to read message(s) from").StringVar(&opts.ActiveMq.Topic)
	cmd.Flag("queue", "Topic to read message(s) from").StringVar(&opts.ActiveMq.Queue)
	cmd.Flag("timeout", "Connect timeout").DurationVar(&opts.ActiveMq.Timeout)
}

func addReadActiveMqFlags(cmd *kingpin.CmdClause, opts *Options) {

}

func addWriteActiveMqFlags(cmd *kingpin.CmdClause, opts *Options) {
}

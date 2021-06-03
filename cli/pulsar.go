package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type PulsarOptions struct {
	// Shared
	Address  string
	Topic    string
	Queue    string
	ClientId string

	// Subscription
	ReadFollow       bool
	SubscriptionName string
	SubscriptionType string

	// Read
	RolePrefix string
}

func HandlePulsarFlags(readCmd, writeCmd, subscribeCmd *kingpin.CmdClause, opts *Options) {
	sc := subscribeCmd.Command("pulsar", "Pulsar")
	addSharedPulsarFlags(sc, opts)
	addSubscribePulsarFlags(sc, opts)

	rc := readCmd.Command("pulsar", "Pulsar")
	addSharedPulsarFlags(rc, opts)
	addReadPulsarFlags(rc, opts)

	wc := writeCmd.Command("pulsar", "Pulsar")
	addSharedPulsarFlags(wc, opts)
}

func addSharedPulsarFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").
		Default("pulsar://localhost:6650").
		StringVar(&opts.Pulsar.Address)

	cmd.Flag("topic", "Topic to read message(s) from").
		StringVar(&opts.Pulsar.Topic)
}

func addReadPulsarFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("start-message-id", "Start reading at this message ID").
		StringVar(&opts.Pulsar.SubscriptionName)

	cmd.Flag("prefix", "Reader role prefix").
		Default("reader").
		StringVar(&opts.Pulsar.RolePrefix)
}

func addSubscribePulsarFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("name", "Subscription Name").
		Required().
		StringVar(&opts.Pulsar.SubscriptionName)

	cmd.Flag("type", "Subscription Type").
		Default("shared").
		EnumVar(&opts.Pulsar.SubscriptionType, "shared", "keyshared", "failover", "exclusive")

}

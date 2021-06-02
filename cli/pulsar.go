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

	// Read
	ReadFollow       bool
	SubscriptionName string
	SubscriptionType string
}

func HandlePulsarFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
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
	cmd.Flag("subscription-name", "Subscription Name").
		StringVar(&opts.Pulsar.SubscriptionName)

	cmd.Flag("subscription-type", "Subscription Type").
		Default("shared").
		EnumVar(&opts.Pulsar.SubscriptionType, "shared", "keyshared", "failover", "exclusive")
}

package options

import (
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

const DefaultPulsarConnectTimeout = "10s"

type PulsarOptions struct {
	// Shared
	Address             string
	Topic               string
	Queue               string
	ClientId            string
	InsecureTLS         bool
	AuthCertificateFile string
	AuthKeyFile         string
	ConnectTimeout      time.Duration

	// Read
	ReadFollow       bool
	SubscriptionName string
	SubscriptionType string
}

func HandlePulsarFlags(readCmd, writeCmd, _ *kingpin.CmdClause, opts *Options) {
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

	cmd.Flag("timeout", "Connect timeout").
		Default(DefaultPulsarConnectTimeout).
		DurationVar(&opts.Pulsar.ConnectTimeout)

	cmd.Flag("auth-certificate-file", "Path of certificate to authenticate with").
		StringVar(&opts.Pulsar.AuthCertificateFile)

	cmd.Flag("auth-key-file", "Path of key for authentication certificate").
		StringVar(&opts.Pulsar.AuthKeyFile)

	cmd.Flag("insecure-tls", "Whether to verify server certificate").
		BoolVar(&opts.Pulsar.InsecureTLS)

	cmd.Flag("topic", "Topic to read messages from or write messages to").
		StringVar(&opts.Pulsar.Topic)
}

func addReadPulsarFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("name", "Subscription Name").
		Required().
		StringVar(&opts.Pulsar.SubscriptionName)

	cmd.Flag("type", "Subscription Type").
		Default("shared").
		EnumVar(&opts.Pulsar.SubscriptionType, "shared", "keyshared", "failover", "exclusive")

}

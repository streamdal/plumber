package cli

import "gopkg.in/alecthomas/kingpin.v2"

type AzureServiceBusOptions struct {
	// Shared
	Queue            string
	ConnectionString string
	Topic            string

	// Read
	Subscription string
}

func HandleAzureFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("azure", "Azure Service Buz")
	addSharedAzureFlags(rc, opts)
	addReadAzureFlags(rc, opts)

	wc := writeCmd.Command("azure", "Azure Service Bus")
	addSharedAzureFlags(wc, opts)
}

func addReadAzureFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("subscription", "Subscription Name").
		StringVar(&opts.Azure.Subscription)
}

func addSharedAzureFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue", "Queue name").
		StringVar(&opts.Azure.Queue)
	cmd.Flag("topic", "Topic name").
		StringVar(&opts.Azure.Topic)
	cmd.Flag("connection-string", "Connection string").
		Envar("SERVICEBUS_CONNECTION_STRING").
		Required().
		StringVar(&opts.Azure.ConnectionString)
}

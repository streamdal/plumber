package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type AzureServiceBusOptions struct {
	// Shared
	Queue            string
	ConnectionString string
	Topic            string

	// Read
	Subscription string
}

func HandleAzureFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("azure", "Azure Service Bus")
	addSharedAzureFlags(rc, opts)
	addReadAzureFlags(rc, opts)

	wc := writeCmd.Command("azure", "Azure Service Bus")
	addSharedAzureFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("azure", "Azure Service Bus")
	}

	addSharedAzureFlags(rec, opts)
	addReadAzureFlags(rec, opts)
}

func addReadAzureFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("subscription", "Subscription Name").
		Envar("PLUMBER_RELAY_AZURE_SUBSCRIPTION").
		StringVar(&opts.Azure.Subscription)
}

func addSharedAzureFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue", "Queue name").
		Envar("PLUMBER_RELAY_AZURE_QUEUE_NAME").
		StringVar(&opts.Azure.Queue)
	cmd.Flag("topic", "Topic name").
		Envar("PLUMBER_RELAY_AZURE_TOPIC_NAME").
		StringVar(&opts.Azure.Topic)
	cmd.Flag("connection-string", "Connection string").
		Envar("SERVICEBUS_CONNECTION_STRING").
		Required().
		StringVar(&opts.Azure.ConnectionString)
}

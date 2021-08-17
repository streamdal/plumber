package options

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type AzureEventHubOptions struct {
	// Shared
	ConnectionString string

	// Write
	MessageID    string
	PartitionKey string
}

func HandleAzureEventHubFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("azure-eventhub", "Azure Event Hub")
	addSharedAzureEventhubFlags(rc, opts)

	wc := writeCmd.Command("azure-eventhub", "Azure Event Hub")
	addSharedAzureEventhubFlags(wc, opts)
	addWriteAzureEventhubFlags(wc, opts)

	rec := relayCmd.Command("azure-eventhub", "Azure Event Hub")
	addSharedAzureEventhubFlags(rec, opts)
}

func addSharedAzureEventhubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("connection-string", "Connection string").
		Envar("EVENTHUB_CONNECTION_STRING").
		Required().
		StringVar(&opts.AzureEventHub.ConnectionString)
}

func addWriteAzureEventhubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("message-id", "Send message with this ID").
		StringVar(&opts.AzureEventHub.MessageID)

	cmd.Flag("partition-key", "Send message with this ID").
		StringVar(&opts.AzureEventHub.PartitionKey)
}

package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type GCPPubSubOptions struct {
	// Shared
	ProjectId       string
	CredentialsFile string
	CredentialsJSON string

	// Read
	ReadSubscriptionId string
	ReadAck            bool

	// Write
	WriteTopicId string
}

func HandleGCPPubSubFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	// GCP PubSub read cmd
	rc := readCmd.Command("gcp-pubsub", "GCP PubSub message system")

	addSharedGCPPubSubFlags(rc, opts)
	addReadGCPPubSubFlags(rc, opts)

	// GCPPubSub write cmd
	wc := writeCmd.Command("gcp-pubsub", "GCP PubSub message system")

	addSharedGCPPubSubFlags(wc, opts)
	addWriteGCPPubSubFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("gcp-pubsub", "GCP PubSub message system")
	}

	addSharedGCPPubSubFlags(rec, opts)
	addReadGCPPubSubFlags(rec, opts)
}

func addSharedGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("credentials-file", "Location to GCP JSON credentials file").
		Envar("GOOGLE_APPLICATION_CREDENTIALS").
		StringVar(&opts.GCPPubSub.CredentialsFile)

	cmd.Flag("credentials-json", "JSON of GCP credentials. Must specify this or --credentials-file").
		Envar("PLUMBER_RELAY_GCP_CREDENTIALS").
		StringVar(&opts.GCPPubSub.CredentialsJSON)

	cmd.Flag("project-id", "Project Id").
		Required().
		Envar("PLUMBER_RELAY_GCP_PROJECT_ID").
		StringVar(&opts.GCPPubSub.ProjectId)
}

func addReadGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("sub-id", "Subscription Id").
		Required().
		Envar("PLUMBER_RELAY_GCP_SUBSCRIPTION_ID").
		StringVar(&opts.GCPPubSub.ReadSubscriptionId)
	cmd.Flag("ack", "Whether to acknowledge message receive").
		Default("true").
		Envar("PLUMBER_RELAY_GCP_ACK_MESSAGE").
		BoolVar(&opts.GCPPubSub.ReadAck)
}

func addWriteGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic-id", "Topic Id to publish message(s) to").Required().
		StringVar(&opts.GCPPubSub.WriteTopicId)
}

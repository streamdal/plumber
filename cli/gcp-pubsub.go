package cli

import "gopkg.in/alecthomas/kingpin.v2"

type GCPPubSubOptions struct {
	// Shared
	ProjectId string

	// Read
	ReadSubscriptionId string
	ReadAck            bool

	// Write
	WriteTopicId string
}

func HandleGCPPubSubFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	// GCP PubSub read cmd
	rc := readCmd.Command("gcp-pubsub", "GCP PubSub message system")

	addSharedGCPPubSubFlags(rc, opts)
	addReadGCPPubSubFlags(rc, opts)

	// GCPPubSub write cmd
	wc := writeCmd.Command("gcp-pubsub", "GCP PubSub message system")

	addSharedGCPPubSubFlags(wc, opts)
	addWriteGCPPubSubFlags(wc, opts)
}

func addSharedGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("project-id", "Project Id").Required().StringVar(&opts.GCPPubSub.ProjectId)
}

func addReadGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("sub-id", "Subscription Id").Required().StringVar(&opts.GCPPubSub.ReadSubscriptionId)
	cmd.Flag("ack", "Whether to acknowledge message receive").Default("true").
		BoolVar(&opts.GCPPubSub.ReadAck)
}

func addWriteGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic-id", "Topic Id to publish message(s) to").Required().
		StringVar(&opts.GCPPubSub.WriteTopicId)
}

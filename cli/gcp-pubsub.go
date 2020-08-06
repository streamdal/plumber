package cli

import "gopkg.in/alecthomas/kingpin.v2"

type GCPPubSubOptions struct {
	// Shared
	ProjectId string

	// Read
	ReadSubscriptionId      string
	ReadProtobufDir         string
	ReadProtobufRootMessage string
	ReadOutputType          string
	ReadAck                 bool
	ReadFollow              bool
	ReadLineNumbers         bool
	ReadConvert             string

	// Write
	WriteTopicId             string
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDir         string
	WriteProtobufRootMessage string
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
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.GCPPubSub.ReadProtobufDir)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(&opts.GCPPubSub.ReadProtobufRootMessage)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(&opts.GCPPubSub.ReadOutputType, "plain", "protobuf")
	cmd.Flag("ack", "Whether to acknowledge message receive").Default("true").
		BoolVar(&opts.GCPPubSub.ReadAck)
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		BoolVar(&opts.GCPPubSub.ReadFollow)
	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.GCPPubSub.ReadLineNumbers)
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.GCPPubSub.ReadConvert, "base64", "gzip")
}

func addWriteGCPPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic-id", "Topic Id to publish message(s) to").Required().
		StringVar(&opts.GCPPubSub.WriteTopicId)
	cmd.Flag("input-data", "Data to write to GCP PubSub").StringVar(&opts.GCPPubSub.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.GCPPubSub.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.GCPPubSub.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.GCPPubSub.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.GCPPubSub.WriteProtobufDir)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(&opts.GCPPubSub.WriteProtobufRootMessage)
}

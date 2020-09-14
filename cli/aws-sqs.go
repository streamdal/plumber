package cli

import "gopkg.in/alecthomas/kingpin.v2"

type AWSSQSOptions struct {
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

func HandleAWSSQSFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	// GCP PubSub read cmd
	rc := readCmd.Command("aws-sqs", "AWS Simple Queue System")

	addSharedAWSSQSFlags(rc, opts)
	addReadAWSSQSFlags(rc, opts)

	// AWSSQS write cmd
	wc := writeCmd.Command("aws-sqs", "AWS Simple Queue System")

	addSharedAWSSQSFlags(wc, opts)
	addWriteAWSSQSFlags(wc, opts)
}

func addSharedAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("project-id", "Project Id").Required().StringVar(&opts.AWSSQS.ProjectId)
}

func addReadAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("sub-id", "Subscription Id").Required().StringVar(&opts.AWSSQS.ReadSubscriptionId)
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.AWSSQS.ReadProtobufDir)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(&opts.AWSSQS.ReadProtobufRootMessage)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(&opts.AWSSQS.ReadOutputType, "plain", "protobuf")
	cmd.Flag("ack", "Whether to acknowledge message receive").Default("true").
		BoolVar(&opts.AWSSQS.ReadAck)
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		BoolVar(&opts.AWSSQS.ReadFollow)
	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.AWSSQS.ReadLineNumbers)
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.AWSSQS.ReadConvert, "base64", "gzip")
}

func addWriteAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic-id", "Topic Id to publish message(s) to").Required().
		StringVar(&opts.AWSSQS.WriteTopicId)
	cmd.Flag("input-data", "Data to write to GCP PubSub").StringVar(&opts.AWSSQS.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.AWSSQS.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.AWSSQS.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.AWSSQS.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.AWSSQS.WriteProtobufDir)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(&opts.AWSSQS.WriteProtobufRootMessage)
}

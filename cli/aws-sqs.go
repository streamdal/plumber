package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type AWSSQSOptions struct {
	// Shared
	QueueName string

	// Read
	ReadMaxNumMessages          int
	ReadAutoDelete              bool
	ReadReceiveRequestAttemptId string
	ReadWaitTimeSeconds         int64
	ReadProtobufDir             string
	ReadProtobufRootMessage     string
	ReadOutputType              string
	ReadFollow                  bool
	ReadLineNumbers             bool
	ReadConvert                 string

	// Write
	WriteDelaySeconds        int64
	WriteAttributes          map[string]string
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDir         string
	WriteProtobufRootMessage string
}

func HandleAWSSQSFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	// AWSSQS read cmd
	rc := readCmd.Command("aws-sqs", "AWS Simple Queue System")

	addSharedAWSSQSFlags(rc, opts)
	addReadAWSSQSFlags(rc, opts)

	// AWSSQS write cmd
	wc := writeCmd.Command("aws-sqs", "AWS Simple Queue System")

	addSharedAWSSQSFlags(wc, opts)
	addWriteAWSSQSFlags(wc, opts)
}

func addSharedAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue-name", "Queue name").Required().StringVar(&opts.AWSSQS.QueueName)
}

func addReadAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("max-num-messages", "Max number of messages to read").
		Short('m').Default("1").IntVar(&opts.AWSSQS.ReadMaxNumMessages)
	cmd.Flag("receive-request-attempt-id", "An id to identify this read request by").
		Default("plumber").StringVar(&opts.AWSSQS.ReadReceiveRequestAttemptId)
	cmd.Flag("auto-delete", "Delete read/received messages").
		BoolVar(&opts.AWSSQS.ReadAutoDelete)
	cmd.Flag("wait-time-seconds", "Number of seconds to wait for messages (not used when using 'follow')").
		Short('w').Default("5").Int64Var(&opts.AWSSQS.ReadWaitTimeSeconds)
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.AWSSQS.ReadProtobufDir)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(&opts.AWSSQS.ReadProtobufRootMessage)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(&opts.AWSSQS.ReadOutputType, "plain", "protobuf")
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		BoolVar(&opts.AWSSQS.ReadFollow)
	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.AWSSQS.ReadLineNumbers)
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.AWSSQS.ReadConvert, "base64", "gzip")
}

func addWriteAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("delay-seconds", "How many seconds to delay message delivery by").
		Default("0").Int64Var(&opts.AWSSQS.WriteDelaySeconds)
	cmd.Flag("attributes", "Add optional attributes to outgoing message (string=string only)").
		StringMapVar(&opts.AWSSQS.WriteAttributes)
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

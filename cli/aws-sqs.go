package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type AWSSQSOptions struct {
	// Shared
	QueueName       string
	RemoteAccountID string

	// Read
	ReadMaxNumMessages          int64
	ReadAutoDelete              bool
	ReadReceiveRequestAttemptId string
	ReadWaitTimeSeconds         int64
	ReadProtobufDirs            []string
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
	WriteProtobufDirs        []string
	WriteProtobufRootMessage string

	// Relay
	RelayMaxNumMessages          int64
	RelayReceiveRequestAttemptId string
	RelayAutoDelete              bool
	RelayWaitTimeSeconds         int64
}

func HandleAWSSQSFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	// AWSSQS read cmd
	rc := readCmd.Command("aws-sqs", "AWS Simple Queue System")

	addSharedAWSSQSFlags(rc, opts)
	addReadAWSSQSFlags(rc, opts)

	// AWSSQS write cmd
	wc := writeCmd.Command("aws-sqs", "AWS Simple Queue System")

	addSharedAWSSQSFlags(wc, opts)
	addWriteAWSSQSFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("aws-sqs", "AWS Simple Queue System")
	}

	addSharedAWSSQSFlags(rec, opts)
	addRelayAWSSQSFlags(rec, opts)
}

func addSharedAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue-name", "Queue name").
		Envar("PLUMBER_RELAY_SQS_QUEUE_NAME").
		StringVar(&opts.AWSSQS.QueueName)

	cmd.Flag("remote-account-id", "Remote AWS Account ID").
		Envar("PLUMBER_RELAY_SQS_REMOTE_ACCOUNT_ID").
		StringVar(&opts.AWSSQS.RemoteAccountID)
}

func addRelayAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("max-num-messages", "Max number of messages to read").
		Short('m').
		Default("1").
		Envar("PLUMBER_RELAY_SQS_MAX_NUM_MESSAGES").
		Int64Var(&opts.AWSSQS.RelayMaxNumMessages)

	cmd.Flag("receive-request-attempt-id", "An id to identify this read request by").
		Default("plumber/relay").
		Envar("PLUMBER_RELAY_SQS_RECEIVE_REQUEST_ATTEMPT_ID").
		StringVar(&opts.AWSSQS.RelayReceiveRequestAttemptId)

	cmd.Flag("auto-delete", "Delete read/received messages").
		Envar("PLUMBER_RELAY_SQS_AUTO_DELETE").
		BoolVar(&opts.AWSSQS.RelayAutoDelete)

	cmd.Flag("wait-time-seconds", "Number of seconds to wait for messages (not used when using 'follow')").
		Short('w').Default("5").
		Envar("PLUMBER_RELAY_SQS_WAIT_TIME_SECONDS").
		Int64Var(&opts.AWSSQS.RelayWaitTimeSeconds)
}

func addReadAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("max-num-messages", "Max number of messages to read").
		Short('m').
		Default("1").
		Int64Var(&opts.AWSSQS.ReadMaxNumMessages)

	cmd.Flag("receive-request-attempt-id", "An id to identify this read request by").
		Default("plumber").
		StringVar(&opts.AWSSQS.ReadReceiveRequestAttemptId)

	cmd.Flag("auto-delete", "Delete read/received messages").
		BoolVar(&opts.AWSSQS.ReadAutoDelete)

	cmd.Flag("wait-time-seconds", "Number of seconds to wait for messages (not used when using 'follow')").
		Short('w').
		Default("5").
		Int64Var(&opts.AWSSQS.ReadWaitTimeSeconds)

	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.AWSSQS.ReadProtobufDirs)

	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").
		StringVar(&opts.AWSSQS.ReadProtobufRootMessage)

	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").
		EnumVar(&opts.AWSSQS.ReadOutputType, "plain", "protobuf")

	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").
		Short('f').
		BoolVar(&opts.AWSSQS.ReadFollow)

	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").
		BoolVar(&opts.AWSSQS.ReadLineNumbers)

	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.AWSSQS.ReadConvert, "base64", "gzip")
}

func addWriteAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("delay-seconds", "How many seconds to delay message delivery by").
		Default("0").
		Int64Var(&opts.AWSSQS.WriteDelaySeconds)

	cmd.Flag("attributes", "Add optional attributes to outgoing message (string=string only)").
		StringMapVar(&opts.AWSSQS.WriteAttributes)

	cmd.Flag("input-data", "Data to write to GCP PubSub").
		StringVar(&opts.AWSSQS.WriteInputData)

	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.AWSSQS.WriteInputFile)

	cmd.Flag("input-type", "Treat input as this type").
		Default("plain").
		EnumVar(&opts.AWSSQS.WriteInputType, "plain", "base64", "jsonpb")

	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").
		EnumVar(&opts.AWSSQS.WriteOutputType, "plain", "protobuf")

	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.AWSSQS.WriteProtobufDirs)

	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").
		StringVar(&opts.AWSSQS.WriteProtobufRootMessage)
}

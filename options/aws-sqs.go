package options

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

	// Write
	WriteDelaySeconds           int64
	WriteAttributes             map[string]string
	WriteMessageGroupID         string
	WriteMessageDeduplicationID string

	// Relay
	RelayMaxNumMessages          int64
	RelayReceiveRequestAttemptId string
	RelayAutoDelete              bool
	RelayWaitTimeSeconds         int64
}

func HandleAWSSQSFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	// AWSSQS read cmd
	rc := readCmd.Command("aws-sqs", "AWS Simple queue System")

	addSharedAWSSQSFlags(rc, opts)
	addReadAWSSQSFlags(rc, opts)

	// AWSSQS write cmd
	wc := writeCmd.Command("aws-sqs", "AWS Simple queue System")

	addSharedAWSSQSFlags(wc, opts)
	addWriteAWSSQSFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("aws-sqs", "AWS Simple queue System")
	}

	addSharedAWSSQSFlags(rec, opts)
	addRelayAWSSQSFlags(rec, opts)
}

func addSharedAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue-name", "queue name").
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
}

func addWriteAWSSQSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("delay-seconds", "How many seconds to delay message delivery by").
		Default("0").
		Int64Var(&opts.AWSSQS.WriteDelaySeconds)

	cmd.Flag("attributes", "Add optional attributes to outgoing message (string=string only)").
		StringMapVar(&opts.AWSSQS.WriteAttributes)

	cmd.Flag("message-group-id", "Message Group ID. For FIFO queues only").
		StringVar(&opts.AWSSQS.WriteMessageGroupID)

	cmd.Flag("message-deduplication-id", "Required when publishing to a FIFO queue that does not have "+
		"content based deduplication enabled").
		StringVar(&opts.AWSSQS.WriteMessageDeduplicationID)
}

package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type RabbitOptions struct {
	// Shared
	Address    string
	Exchange   string
	RoutingKey string

	// Read
	ReadQueue               string
	ReadQueueDurable        bool
	ReadQueueAutoDelete     bool
	ReadQueueExclusive      bool
	ReadLineNumbers         bool
	ReadFollow              bool
	ReadAutoAck             bool
	ReadProtobufDirs        []string
	ReadProtobufRootMessage string
	ReadOutputType          string
	ReadConvert             string
	ReadQueueDeclare        bool

	// Write
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDirs        []string
	WriteProtobufRootMessage string
}

func HandleRabbitFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	// RabbitMQ read cmd
	rc := readCmd.Command("rabbit", "RabbitMQ message system")

	addSharedRabbitFlags(rc, opts)
	addReadRabbitFlags(rc, opts)

	// Rabbit write cmd
	wc := writeCmd.Command("rabbit", "RabbitMQ message system")

	addSharedRabbitFlags(wc, opts)
	addWriteRabbitFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("rabbit", "RabbitMQ")
	}

	addSharedRabbitFlags(rec, opts)
}

func addSharedRabbitFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("amqp://localhost").
		Envar("PLUMBER_RELAY_RABBIT_ADDRESS").
		StringVar(&opts.Rabbit.Address)
	cmd.Flag("exchange", "Name of the exchange").
		Envar("PLUMBER_RELAY_RABBIT_EXCHANGE").
		StringVar(&opts.Rabbit.Exchange)

	// TODO: This should really NOT be a shared key (for reads - binding key, for writes, routing key)
	cmd.Flag("routing-key", "Routing key").
		Envar("PLUMBER_RELAY_RABBIT_ROUTING_KEY").
		StringVar(&opts.Rabbit.RoutingKey)
	cmd.Flag("queue", "Name of the queue where messages will be routed to").
		Envar("PLUMBER_RELAY_RABBIT_QUEUE").
		StringVar(&opts.Rabbit.ReadQueue)
	cmd.Flag("queue-durable", "Whether the queue we declare should survive server restarts").
		Default("false").
		Envar("PLUMBER_RELAY_RABBIT_QUEUE_DURABLE").
		BoolVar(&opts.Rabbit.ReadQueueDurable)
	cmd.Flag("queue-auto-delete", "Whether to auto-delete the queue after plumber has disconnected").
		Default("true").
		Envar("PLUMBER_RELAY_RABBIT_QUEUE_AUTO_DELETE").
		BoolVar(&opts.Rabbit.ReadQueueAutoDelete)
	cmd.Flag("queue-exclusive", "Whether plumber should be the only one using the newly defined queue").
		Default("true").
		Envar("PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE").
		BoolVar(&opts.Rabbit.ReadQueueExclusive)
	cmd.Flag("auto-ack", "Automatically acknowledge receipt of read/received messages").
		Envar("PLUMBER_RELAY_RABBIT_AUTOACK").
		Default("true").
		BoolVar(&opts.Rabbit.ReadAutoAck)
	cmd.Flag("queue-declare", "Wether to declare the specified queue to create it").
		Envar("PLUMBER_RELAY_RABBIT_QUEUE_DECLARE").
		Default("true").
		BoolVar(&opts.Rabbit.ReadQueueDeclare)
}

func addReadRabbitFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.Rabbit.ReadLineNumbers)
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		Default("false").
		BoolVar(&opts.Rabbit.ReadFollow)
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.Rabbit.ReadProtobufDirs)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(&opts.Rabbit.ReadProtobufRootMessage)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(&opts.Rabbit.ReadOutputType, "plain", "protobuf")
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.Rabbit.ReadConvert, "base64", "gzip")
}

func addWriteRabbitFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("input-data", "Data to write to RabbitMQ").StringVar(&opts.Rabbit.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.Rabbit.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.Rabbit.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.Rabbit.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.Rabbit.WriteProtobufDirs)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(&opts.Rabbit.WriteProtobufRootMessage)
}

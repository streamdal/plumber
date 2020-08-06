package cli

import (
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
	ReadProtobufDir         string
	ReadProtobufRootMessage string
	ReadOutputType          string
	ReadConvert             string

	// Write
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDir         string
	WriteProtobufRootMessage string
}

func HandleRabbitFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	// RabbitMQ read cmd
	rc := readCmd.Command("rabbit", "RabbitMQ message system")

	addSharedRabbitFlags(rc, opts)
	addReadRabbitFlags(rc, opts)

	// Rabbit write cmd
	wc := writeCmd.Command("rabbit", "RabbitMQ message system")

	addSharedRabbitFlags(wc, opts)
	addWriteRabbitFlags(wc, opts)
}

func addSharedRabbitFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("amqp://localhost").
		StringVar(&opts.Rabbit.Address)
	cmd.Flag("exchange", "Name of the exchange").StringVar(&opts.Rabbit.Exchange)

	// TODO: This should really NOT be a shared key (for reads - binding key, for writes, routing key)
	cmd.Flag("routing-key", "Routing key").StringVar(&opts.Rabbit.RoutingKey)
}

func addReadRabbitFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue", "Name of the queue where messages will be routed to").
		StringVar(&opts.Rabbit.ReadQueue)
	cmd.Flag("queue-durable", "Whether the queue we declare should survive server restarts").
		Default("false").BoolVar(&opts.Rabbit.ReadQueueDurable)
	cmd.Flag("queue-auto-delete", "Whether to auto-delete the queue after plumber has disconnected").
		Default("true").BoolVar(&opts.Rabbit.ReadQueueAutoDelete)
	cmd.Flag("queue-exclusive", "Whether plumber should be the only one using the newly defined queue").
		Default("true").BoolVar(&opts.Rabbit.ReadQueueExclusive)
	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.Rabbit.ReadLineNumbers)
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		BoolVar(&opts.Rabbit.ReadFollow)
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.Rabbit.ReadProtobufDir)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(&opts.Rabbit.ReadProtobufRootMessage)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(&opts.Rabbit.ReadOutputType, "plain", "protobuf")
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.Rabbit.ReadConvert, "base64", "gzip")
}

func addWriteRabbitFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("input-data", "Data to write to kafka").StringVar(&opts.Rabbit.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.Rabbit.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.Rabbit.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.Rabbit.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.Rabbit.WriteProtobufDir)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(&opts.Rabbit.WriteProtobufRootMessage)
}

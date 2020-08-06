package cli

import (
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/plumber/backends/kafka"
)

type KafkaOptions struct {
	// Shared
	Address     *string
	Topic       *string
	Timeout     *time.Duration
	InsecureTLS *bool
	LineNumbers *bool

	// Read
	ReadGroupId             *string
	ReadFollow              *bool
	ReadOutputType          *string
	ReadProtobufDir         *string
	ReadProtobufRootMessage *string
	ReadConvert             *string

	// Write
	WriteKey                 *string
	WriteInputData           *string
	WriteInputFile           *string
	WriteInputType           *string
	WriteOutputType          *string
	WriteProtobufDir         *string
	WriteProtobufRootMessage *string
}

func HandleKafkaFlags(app *kingpin.Application, opts *Options) {
	kafkaReadCmd := app.
		Command("read", "Read message(s) from messaging system").
		Command("message", "What to read off of messaging systems").Alias("messages").
		Command("kafka", "Kafka message system")

	addSharedKafkaFlags(kafkaReadCmd, opts)
	addReadKafkaFlags(kafkaReadCmd, opts)

	// Kafka write cmd
	kafkaWriteCmd := app.
		Command("write", "Write message(s) to messaging system").
		Command("message", "What to write to messaging system").Alias("messages").
		Command("kafka", "Kafka message system")

	addSharedKafkaFlags(kafkaWriteCmd, opts)
	addWriteKafkaFlags(kafkaWriteCmd, opts)

}

func addSharedKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("localhost:9092").
		StringVar(opts.Kafka.Address)
	cmd.Flag("topic", "Topic to read message(s) from").Required().StringVar(opts.Kafka.Topic)
	cmd.Flag("timeout", "Connect timeout").Default(kafka.DefaultConnectTimeout).
		DurationVar(opts.Kafka.Timeout)
	cmd.Flag("insecure-tls", "Use insecure TLS").BoolVar(opts.Kafka.InsecureTLS)
	cmd.Flag("line-numbers", "Prepend number to each output message").BoolVar(opts.Kafka.LineNumbers)
}

func addReadKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("group-id", "Specify a specific group-id to use when reading from kafka").
		Default(kafka.DefaultGroupId).StringVar(opts.Kafka.ReadGroupId)
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		BoolVar(opts.Kafka.ReadFollow)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(opts.Kafka.ReadOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(opts.Kafka.ReadProtobufDir)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(opts.Kafka.ReadProtobufRootMessage)
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(opts.Kafka.ReadConvert, "base64", "gzip")
}

func addWriteKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("key", "Key to write to kafka (not required)").StringVar(opts.Kafka.WriteKey)
	cmd.Flag("input-data", "Data to write to kafka").StringVar(opts.Kafka.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(opts.Kafka.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(opts.Kafka.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(opts.Kafka.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(opts.Kafka.WriteProtobufDir)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(opts.Kafka.WriteProtobufRootMessage)
}

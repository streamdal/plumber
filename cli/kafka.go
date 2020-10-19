package cli

import (
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	KafkaDefaultConnectTimeout = "10s"
	KafkaDefaultGroupId        = "plumber"
)

type KafkaOptions struct {
	// Shared
	Address     string
	Topic       string
	Timeout     time.Duration
	InsecureTLS bool

	// Read
	ReadGroupId string

	// Write
	WriteKey                 string
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDirs        []string
	WriteProtobufRootMessage string
}

func HandleKafkaFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("kafka", "Kafka message system")

	addSharedKafkaFlags(rc, opts)
	addReadKafkaFlags(rc, opts)

	// Kafka write cmd
	wc := writeCmd.Command("kafka", "Kafka message system")

	addSharedKafkaFlags(wc, opts)
	addWriteKafkaFlags(wc, opts)
}

func addSharedKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("localhost:9092").StringVar(&opts.Kafka.Address)
	cmd.Flag("topic", "Topic to read message(s) from").Required().StringVar(&opts.Kafka.Topic)
	cmd.Flag("timeout", "Connect timeout").Default(KafkaDefaultConnectTimeout).
		DurationVar(&opts.Kafka.Timeout)
	cmd.Flag("insecure-tls", "Use insecure TLS").BoolVar(&opts.Kafka.InsecureTLS)
}

func addReadKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("group-id", "Specify a specific group-id to use when reading from kafka").
		Default(KafkaDefaultGroupId).StringVar(&opts.Kafka.ReadGroupId)
}

func addWriteKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("key", "Key to write to kafka (not required)").StringVar(&opts.Kafka.WriteKey)
	cmd.Flag("input-data", "Data to write to kafka").StringVar(&opts.Kafka.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.Kafka.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.Kafka.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.Kafka.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.Kafka.WriteProtobufDirs)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(&opts.Kafka.WriteProtobufRootMessage)
}

package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/plumber/util"
)

const (
	DefaultGRPCAddress         = "grpc-collector.batch.sh:9000"
	DefaultHTTPListenAddress   = ":8080"
	DefaultGRPCTimeout         = "10s"
	DefaultNumWorkers          = "10"
	DefaultStatsReportInterval = "5s"
	DefaultBatchSize           = "10"
)

var (
	version = "UNSET"
)

type Options struct {
	// Global
	Debug               bool
	Quiet               bool
	Stats               bool
	StatsReportInterval time.Duration
	Action              string
	Version             string
	Backend             string

	// Serializers
	AvroSchemaFile string

	// Relay
	RelayToken             string
	RelayGRPCAddress       string
	RelayType              string
	RelayHTTPListenAddress string
	RelayNumWorkers        int
	RelayGRPCTimeout       time.Duration
	RelayGRPCDisableTLS    bool
	RelayBatchSize         int

	// Shared read flags
	ReadProtobufRootMessage string
	ReadProtobufDirs        []string
	ReadOutputType          string
	ReadFollow              bool
	ReadLineNumbers         bool
	ReadConvert             string

	// Shared write flags
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDirs        []string
	WriteProtobufRootMessage string

	Kafka     *KafkaOptions
	Rabbit    *RabbitOptions
	GCPPubSub *GCPPubSubOptions
	MQTT      *MQTTOptions
	AWSSQS    *AWSSQSOptions
	AWSSNS    *AWSSNSOptions
	ActiveMq  *ActiveMqOptions
	Redis     *RedisOptions
	Azure     *AzureServiceBusOptions
	Nats      *NatsOptions
}

func Handle(cliArgs []string) (string, *Options, error) {
	opts := &Options{
		Kafka:     &KafkaOptions{},
		Rabbit:    &RabbitOptions{},
		GCPPubSub: &GCPPubSubOptions{},
		MQTT:      &MQTTOptions{},
		AWSSQS:    &AWSSQSOptions{WriteAttributes: make(map[string]string, 0)},
		AWSSNS:    &AWSSNSOptions{},
		ActiveMq:  &ActiveMqOptions{},
		Redis:     &RedisOptions{},
		Azure:     &AzureServiceBusOptions{},
		Nats:      &NatsOptions{},
	}

	app := kingpin.New("plumber", "`curl` for messaging systems. See: https://github.com/batchcorp/plumber")

	// Global (apply to all actions)
	app.Flag("debug", "Enable debug output").
		Short('d').
		Envar("PLUMBER_DEBUG").
		BoolVar(&opts.Debug)

	app.Flag("quiet", "Suppress non-essential output").
		Short('q').
		BoolVar(&opts.Quiet)

	app.Flag("stats", "Display periodic read/write/relay stats").
		Envar("PLUMBER_STATS").
		BoolVar(&opts.Stats)

	app.Flag("stats-report-interval", "Display periodic read/write/relay stats").
		Envar("PLUMBER_STATS_REPORT_INTERVAL").
		Default(DefaultStatsReportInterval).
		DurationVar(&opts.StatsReportInterval)

	// Specific actions
	readCmd := app.Command("read", "Read message(s) from messaging system")
	writeCmd := app.Command("write", "Write message(s) to messaging system")
	relayCmd := app.Command("relay", "Relay message(s) from messaging system to Batch")

	switch os.Getenv("PLUMBER_RELAY_TYPE") {
	case "kafka":
		HandleRelayFlags(relayCmd, opts)
		HandleKafkaFlags(readCmd, writeCmd, relayCmd, opts)
	case "rabbit":
		HandleRelayFlags(relayCmd, opts)
		HandleRabbitFlags(readCmd, writeCmd, relayCmd, opts)
	case "aws-sqs":
		HandleRelayFlags(relayCmd, opts)
		HandleAWSSQSFlags(readCmd, writeCmd, relayCmd, opts)
	case "azure":
		HandleRelayFlags(relayCmd, opts)
		HandleAzureFlags(readCmd, writeCmd, relayCmd, opts)
	case "gcp-pubsup":
		HandleRelayFlags(relayCmd, opts)
		HandleGCPPubSubFlags(readCmd, writeCmd, relayCmd, opts)
	default:
		HandleRelayFlags(relayCmd, opts)
		HandleKafkaFlags(readCmd, writeCmd, relayCmd, opts)
		HandleRabbitFlags(readCmd, writeCmd, relayCmd, opts)
		HandleGCPPubSubFlags(readCmd, writeCmd, relayCmd, opts)
		HandleMQTTFlags(readCmd, writeCmd, opts)
		HandleAWSSQSFlags(readCmd, writeCmd, relayCmd, opts)
		HandleActiveMqFlags(readCmd, writeCmd, opts)
		HandleAWSSNSFlags(readCmd, writeCmd, relayCmd, opts)
		HandleAzureFlags(readCmd, writeCmd, relayCmd, opts)
		HandleNatsFlags(readCmd, writeCmd, relayCmd, opts)
		HandleRedisFlags(readCmd, writeCmd, relayCmd, opts)
	}

	HandleGlobalFlags(readCmd, opts)
	HandleGlobalReadFlags(readCmd, opts)
	HandleGlobalWriteFlags(writeCmd, opts)
	HandleGlobalReadFlags(relayCmd, opts)
	HandleGlobalFlags(writeCmd, opts)
	HandleGlobalFlags(relayCmd, opts)

	app.Version(version)
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	cmd, err := app.Parse(cliArgs)
	if err != nil {
		return "", nil, errors.Wrap(err, "unable to parse command")
	}

	opts.Action = "unknown"
	opts.Version = version

	cmds := strings.Split(cmd, " ")
	if len(cmds) > 0 {
		opts.Action = cmds[0]
	}

	return cmd, opts, err
}

func HandleGlobalReadFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").
		StringVar(&opts.ReadProtobufRootMessage)

	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.ReadProtobufDirs)

	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").
		EnumVar(&opts.ReadOutputType, "plain", "protobuf")

	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").
		Short('f').
		BoolVar(&opts.ReadFollow)

	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.ReadLineNumbers)

	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.ReadConvert, "base64", "gzip")
}

func HandleGlobalWriteFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("input-data", "Data to write").StringVar(&opts.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		Envar("PLUMBER_RELAY_PROTOBUF_DIR").
		ExistingDirsVar(&opts.WriteProtobufDirs)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").
		Envar("PLUMBER_RELAY_PROTOBUF_ROOT_MESSAGE").
		StringVar(&opts.WriteProtobufRootMessage)
}

func HandleGlobalFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("avro-schema", "Path to AVRO schema .avsc file").
		StringVar(&opts.AvroSchemaFile)
}

func HandleRelayFlags(relayCmd *kingpin.CmdClause, opts *Options) {
	relayCmd.Flag("type", "Type of collector to use. Ex: rabbit, kafka, aws-sqs, azure, gcp-pubsub").
		Envar("PLUMBER_RELAY_TYPE").
		EnumVar(&opts.RelayType, "aws-sqs", "rabbit", "kafka", "azure", "gcp-pubsub")

	relayCmd.Flag("token", "Collection token to use when sending data to Batch").
		Required().
		Envar("PLUMBER_RELAY_TOKEN").
		StringVar(&opts.RelayToken)

	relayCmd.Flag("grpc-address", "Alternative gRPC collector address").
		Default(DefaultGRPCAddress).
		Envar("PLUMBER_RELAY_GRPC_ADDRESS").
		StringVar(&opts.RelayGRPCAddress)

	relayCmd.Flag("grpc-disable-tls", "Disable TLS when talking to gRPC collector").
		Default("false").
		Envar("PLUMBER_RELAY_GRPC_DISABLE_TLS").
		BoolVar(&opts.RelayGRPCDisableTLS)

	relayCmd.Flag("grpc-timeout", "gRPC collector timeout").
		Default(DefaultGRPCTimeout).
		Envar("PLUMBER_RELAY_GRPC_TIMEOUT").
		DurationVar(&opts.RelayGRPCTimeout)

	relayCmd.Flag("num-workers", "Number of relay workers").
		Default(DefaultNumWorkers).
		Envar("PLUMBER_RELAY_NUM_WORKERS").
		IntVar(&opts.RelayNumWorkers)

	relayCmd.Flag("listen-address", "Alternative listen address for local HTTP server").
		Default(DefaultHTTPListenAddress).
		Envar("PLUMBER_RELAY_HTTP_LISTEN_ADDRESS").
		StringVar(&opts.RelayHTTPListenAddress)

	relayCmd.Flag("batch-size", "How many messages to batch before sending them to grpc-collector").
		Default(DefaultBatchSize).
		Envar("PLUMBER_RELAY_BATCH_SIZE").
		IntVar(&opts.RelayBatchSize)
}

func ValidateProtobufOptions(dirs []string, rootMessage string) error {
	if len(dirs) == 0 {
		return errors.New("at least one '--protobuf-dir' required when type " +
			"is set to 'protobuf'")
	}

	if rootMessage == "" {
		return errors.New("'--protobuf-root-message' required when " +
			"type is set to 'protobuf'")
	}

	// Does given dir exist?
	if err := util.DirsExist(dirs); err != nil {
		return fmt.Errorf("--protobuf-dir validation error(s): %s", err)
	}

	return nil
}

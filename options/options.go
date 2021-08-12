// Package options is a common options interface that is used by CLI, backends,
// and the gRPC server. Its purpose is primarily to store all available options
// for plumber - its other responsibilities are to perform "light" validation.
//
// Additional validation should be performed by the utilizers of the options
// package.
package options

import (
	"os"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	DefaultGRPCAddress         = "grpc-collector.batch.sh:9000"
	DefaultHTTPListenAddress   = ":8080"
	DefaultGRPCTimeout         = "10s"
	DefaultNumWorkers          = "10"
	DefaultStatsReportInterval = "5s"
	DefaultCount               = "10"
	DefaultDproxyAddress       = "dproxy.batch.sh:443"

	STDOUT OutputDestination = iota
	CHANNEL
)

var (
	version = "UNSET"
)

type Options struct {
	// Global options
	Debug               bool
	Quiet               bool
	Stats               bool
	StatsReportInterval time.Duration
	Action              string
	Version             string

	// Read options include settings for reading data
	Read *ReadOptions

	// Write options include settings for writing data
	Write *WriteOptions

	// Server options include settings for running plumber in server-mode
	Server *ServerOptions

	// Relay options include settings for running plumber in relay-mode
	Relay *RelayOptions

	// DProxy options include settings for running plumber in dproxy-mode
	DProxy *DProxyOptions

	// Encoding options include settings that will affect how Write will write data
	Encoding *EncodingOptions

	// Decoding options include settings that will affect how Read will read data
	Decoding *DecodingOptions

	// Batch options include settings for working with the Batch API
	Batch *BatchOptions

	// Backends
	Kafka           *KafkaOptions
	Rabbit          *RabbitOptions
	RabbitMQStreams *RabbitMQStreamsOptions
	GCPPubSub       *GCPPubSubOptions
	MQTT            *MQTTOptions
	AWSSQS          *AWSSQSOptions
	AWSSNS          *AWSSNSOptions
	ActiveMq        *ActiveMqOptions
	RedisPubSub     *RedisPubSubOptions
	RedisStreams    *RedisStreamsOptions
	Azure           *AzureServiceBusOptions
	AzureEventHub   *AzureEventHubOptions
	Nats            *NatsOptions
	NatsStreaming   *NatsStreamingOptions
	CDCMongo        *CDCMongoOptions
	CDCPostgres     *CDCPostgresOptions
	Pulsar          *PulsarOptions
	NSQ             *NSQOptions
}

type OutputDestination int

type ReadOptions struct {
	Follow  bool
	Lag     bool
	Convert string
	Verbose bool
}

type WriteOptions struct {
	InputData        []string
	InputFile        string
	InputType        string
	InputIsJsonArray bool
}

type RelayOptions struct {
	Token             string
	GRPCAddress       string
	Type              string
	HTTPListenAddress string
	NumWorkers        int
	GRPCTimeout       time.Duration
	GRPCDisableTLS    bool
	BatchSize         int
}

type DProxyOptions struct {
	APIToken    string
	Address     string
	Insecure    bool
	GRPCTimeout time.Duration
}

type EncodingOptions struct {
	ProtobufRootMessage string
	ProtobufDirs        []string
	AvroSchemaFile      string

	// Set _after_ plumber instantiation
	MsgDesc *desc.MessageDescriptor
}

type DecodingOptions struct {
	ProtobufRootMessage string
	ProtobufDirs        []string
	JSONOutput          bool
	ThriftOutput        bool
	AvroSchemaFile      string

	// Set _after_ plumber instantiation
	MsgDesc *desc.MessageDescriptor
}

func Handle(cliArgs []string) (string, *Options, error) {
	opts := &Options{
		// Instantiate main configs
		Read:     &ReadOptions{},
		Write:    &WriteOptions{},
		Encoding: &EncodingOptions{},
		Decoding: &DecodingOptions{},
		Server:   &ServerOptions{},
		DProxy:   &DProxyOptions{},
		Relay:    &RelayOptions{},
		Batch: &BatchOptions{
			DestinationMetadata: &DestinationMetadata{
				HTTPHeaders: make(map[string]string, 0),
			},
		},

		// Instantiate backend configs
		Kafka: &KafkaOptions{
			WriteHeader: make(map[string]string, 0),
		},
		Rabbit:          &RabbitOptions{},
		RabbitMQStreams: &RabbitMQStreamsOptions{},
		GCPPubSub:       &GCPPubSubOptions{},
		MQTT:            &MQTTOptions{},
		AWSSQS: &AWSSQSOptions{
			WriteAttributes: make(map[string]string, 0),
		},
		AWSSNS:        &AWSSNSOptions{},
		ActiveMq:      &ActiveMqOptions{},
		RedisPubSub:   &RedisPubSubOptions{},
		RedisStreams:  &RedisStreamsOptions{},
		Azure:         &AzureServiceBusOptions{},
		AzureEventHub: &AzureEventHubOptions{},
		Nats:          &NatsOptions{},
		NatsStreaming: &NatsStreamingOptions{},
		CDCMongo:      &CDCMongoOptions{},
		CDCPostgres:   &CDCPostgresOptions{},
		Pulsar:        &PulsarOptions{},
		NSQ:           &NSQOptions{},
	}

	app := kingpin.New("plumber", "`curl` for messaging systems. See: https://github.com/batchcorp/plumber")

	// Specific actions
	readCmd := app.Command("read", "Read message(s) from messaging system")
	writeCmd := app.Command("write", "Write message(s) to messaging system")
	relayCmd := app.Command("relay", "Relay message(s) from messaging system to Batch")
	batchCmd := app.Command("batch", "Access your Batch.sh account information")
	lagCmd := app.Command("lag", "Monitor lag in the messaging system")
	dynamicCmd := app.Command("dynamic", "Act as a batch.sh replay destination")
	githubCmd := app.Command("github", "Authorize plumber to access your github repos")
	serverCmd := app.Command("server", "Run plumber in server mode")

	HandleRelayFlags(relayCmd, opts)

	switch os.Getenv("PLUMBER_RELAY_TYPE") {
	case "kafka":
		HandleKafkaFlags(readCmd, writeCmd, relayCmd, lagCmd, opts)
	case "rabbit":
		HandleRabbitFlags(readCmd, writeCmd, relayCmd, opts)
	case "rabbit-streams":
		HandleRabbitStreamsFlags(readCmd, writeCmd, relayCmd, opts)
	case "aws-sqs":
		HandleAWSSQSFlags(readCmd, writeCmd, relayCmd, opts)
	case "azure":
		HandleAzureFlags(readCmd, writeCmd, relayCmd, opts)
	case "gcp-pubsup":
		HandleGCPPubSubFlags(readCmd, writeCmd, relayCmd, opts)
	case "redis-pubsub":
		HandleRedisPubSubFlags(readCmd, writeCmd, relayCmd, opts)
	case "redis-streams":
		HandleRedisStreamsFlags(readCmd, writeCmd, relayCmd, opts)
	case "cdc-postgres":
		HandleCDCPostgresFlags(readCmd, writeCmd, relayCmd, opts)
	case "cdc-mongo":
		HandleCDCMongoFlags(readCmd, writeCmd, relayCmd, opts)
	case "mqtt":
		HandleMQTTFlags(readCmd, writeCmd, relayCmd, opts)
	default:
		HandleKafkaFlags(readCmd, writeCmd, relayCmd, lagCmd, opts)
		HandleRabbitFlags(readCmd, writeCmd, relayCmd, opts)
		HandleRabbitStreamsFlags(readCmd, writeCmd, relayCmd, opts)
		HandleGCPPubSubFlags(readCmd, writeCmd, relayCmd, opts)
		HandleMQTTFlags(readCmd, writeCmd, relayCmd, opts)
		HandleAWSSQSFlags(readCmd, writeCmd, relayCmd, opts)
		HandleActiveMqFlags(readCmd, writeCmd, opts)
		HandleAWSSNSFlags(readCmd, writeCmd, relayCmd, opts)
		HandleAzureFlags(readCmd, writeCmd, relayCmd, opts)
		HandleAzureEventHubFlags(readCmd, writeCmd, relayCmd, opts)
		HandleNatsFlags(readCmd, writeCmd, relayCmd, opts)
		HandleNatsStreamingFlags(readCmd, writeCmd, relayCmd, opts)
		HandleRedisPubSubFlags(readCmd, writeCmd, relayCmd, opts)
		HandleRedisStreamsFlags(readCmd, writeCmd, relayCmd, opts)
		HandleCDCMongoFlags(readCmd, writeCmd, relayCmd, opts)
		HandleCDCPostgresFlags(readCmd, writeCmd, relayCmd, opts)
		HandleDynamicFlags(dynamicCmd, opts)
		HandlePulsarFlags(readCmd, writeCmd, relayCmd, opts)
		HandleNSQFlags(readCmd, writeCmd, relayCmd, opts)
	}

	HandleGlobalFlags(readCmd, opts)
	HandleReadFlags(readCmd, opts)
	HandleWriteFlags(writeCmd, opts)
	HandleReadFlags(relayCmd, opts)
	HandleServerFlags(serverCmd, opts)
	HandleGlobalFlags(writeCmd, opts)
	HandleGlobalFlags(relayCmd, opts)
	HandleGlobalFlags(dynamicCmd, opts)
	HandleBatchFlags(batchCmd, opts)
	HandleGlobalDynamicFlags(dynamicCmd, opts)
	HandleGithubFlags(githubCmd, opts)

	app.Version(version)
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	cmd, err := app.Parse(cliArgs)
	if err != nil {
		return "", nil, errors.Wrap(err, "unable to parse command")
	}

	// Hack: kingpin requires multiple values to be separated by newline which
	// is not great for env vars so we use a comma instead
	convertSliceArgs(opts)

	opts.Action = "unknown"
	opts.Version = version

	cmds := strings.Split(cmd, " ")
	if len(cmds) > 0 {
		opts.Action = cmds[0]
	}

	return cmd, opts, err
}

// convertSliceArgs splits up comma delimited flags into a slice. We do this because slice argument
// environment variables in kingpin are newline delimited for some odd reason
// See https://github.com/alecthomas/kingpin/issues/257
func convertSliceArgs(opts *Options) {
	if len(opts.RedisPubSub.Channels) == 1 {
		opts.RedisPubSub.Channels = strings.Split(opts.RedisPubSub.Channels[0], ",")
	}

	if len(opts.RedisStreams.Streams) == 1 {
		opts.RedisStreams.Streams = strings.Split(opts.RedisStreams.Streams[0], ",")
	}

	if len(opts.Kafka.Brokers) == 1 && strings.Contains(opts.Kafka.Brokers[0], ",") {
		opts.Kafka.Brokers = strings.Split(opts.Kafka.Brokers[0], ",")
	}

	if len(opts.Kafka.Topics) == 1 && strings.Contains(opts.Kafka.Topics[0], ",") {
		opts.Kafka.Topics = strings.Split(opts.Kafka.Topics[0], ",")
	}
}

func HandleReadFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").
		StringVar(&opts.Decoding.ProtobufRootMessage)

	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirsVar(&opts.Decoding.ProtobufDirs)

	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").
		Short('f').
		BoolVar(&opts.Read.Follow)

	cmd.Flag("convert", "Convert received message(s) [base64, gzip]").
		EnumVar(&opts.Read.Convert, "base64", "gzip")

	cmd.Flag("verbose", "Display message metadata if available").
		BoolVar(&opts.Read.Verbose)

	cmd.Flag("lag", "Display amount of messages with un-commited offset, if different from the previous message").
		Default("false").
		BoolVar(&opts.Read.Lag)

	cmd.Flag("json", "Read data should be treated as JSON").
		Default("false").
		BoolVar(&opts.Decoding.JSONOutput)

	cmd.Flag("thrift", "Read data as a thrift encoded message").
		Default("false").
		BoolVar(&opts.Decoding.ThriftOutput)
}

func HandleGlobalDynamicFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("api-token", "Batch.SH API Token").
		StringVar(&opts.DProxy.APIToken)

	cmd.Flag("dproxy-address", "Address of Batch.sh's Dynamic Destination server").
		Default(DefaultDproxyAddress).
		StringVar(&opts.DProxy.Address)

	cmd.Flag("grpc-timeout", "dProxy gRPC server timeout").
		Default(DefaultGRPCTimeout).
		DurationVar(&opts.DProxy.GRPCTimeout)

	cmd.Flag("dproxy-insecure", "Connect to dProxy server without TLS").
		BoolVar(&opts.DProxy.Insecure)
}

func HandleWriteFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("input-data", "Data to write").
		StringsVar(&opts.Write.InputData)

	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.Write.InputFile)

	cmd.Flag("input-type", "Treat input-file as this type [plain, base64, jsonpb]").
		Default("plain").
		EnumVar(&opts.Write.InputType, "plain", "base64", "jsonpb")

	cmd.Flag("protobuf-dir", "Directory with .proto files").
		Envar("PLUMBER_RELAY_PROTOBUF_DIR").
		ExistingDirsVar(&opts.Encoding.ProtobufDirs)

	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set; type should contain pkg name(s) separated by a period)").
		Envar("PLUMBER_RELAY_PROTOBUF_ROOT_MESSAGE").
		StringVar(&opts.Encoding.ProtobufRootMessage)

	cmd.Flag("json-array", "Handle input as JSON array instead of newline delimited data. "+
		"Each array element will be written as a separate item").
		BoolVar(&opts.Write.InputIsJsonArray)
}

func HandleGlobalFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("debug", "Enable debug output").
		Short('d').
		Envar("PLUMBER_DEBUG").
		BoolVar(&opts.Debug)

	cmd.Flag("quiet", "Suppress non-essential output").
		Short('q').
		BoolVar(&opts.Quiet)

	cmd.Flag("stats", "Display periodic read/write/relay stats").
		Envar("PLUMBER_STATS").
		BoolVar(&opts.Stats)

	cmd.Flag("stats-report-interval", "Interval at which periodic stats are displayed").
		Envar("PLUMBER_STATS_REPORT_INTERVAL").
		Default(DefaultStatsReportInterval).
		DurationVar(&opts.StatsReportInterval)

	cmd.Flag("avro-schema", "Path to AVRO schema .avsc file").
		StringVar(&opts.Decoding.AvroSchemaFile) // TODO: This is both, encoding and decoding, not global
}

func HandleRelayFlags(relayCmd *kingpin.CmdClause, opts *Options) {
	relayCmd.Flag("type", "Type of collector to use. Ex: rabbit, kafka, aws-sqs, azure, gcp-pubsub, redis-pubsub, redis-streams").
		Envar("PLUMBER_RELAY_TYPE").
		EnumVar(&opts.Relay.Type, "aws-sqs", "rabbit", "kafka", "azure", "gcp-pubsub", "redis-pubsub",
			"redis-streams", "cdc-postgres", "cdc-mongo", "mqtt")

	relayCmd.Flag("token", "Collection token to use when sending data to Batch").
		Required().
		Envar("PLUMBER_RELAY_TOKEN").
		StringVar(&opts.Relay.Token)

	relayCmd.Flag("grpc-address", "Alternative gRPC collector address").
		Default(DefaultGRPCAddress).
		Envar("PLUMBER_RELAY_GRPC_ADDRESS").
		StringVar(&opts.Relay.GRPCAddress)

	relayCmd.Flag("grpc-disable-tls", "Disable TLS when talking to gRPC collector").
		Default("false").
		Envar("PLUMBER_RELAY_GRPC_DISABLE_TLS").
		BoolVar(&opts.Relay.GRPCDisableTLS)

	relayCmd.Flag("grpc-timeout", "gRPC collector timeout").
		Default(DefaultGRPCTimeout).
		Envar("PLUMBER_RELAY_GRPC_TIMEOUT").
		DurationVar(&opts.Relay.GRPCTimeout)

	relayCmd.Flag("num-workers", "Number of relay workers").
		Default(DefaultNumWorkers).
		Envar("PLUMBER_RELAY_NUM_WORKERS").
		IntVar(&opts.Relay.NumWorkers)

	relayCmd.Flag("listen-address", "Alternative listen address for local HTTP server").
		Default(DefaultHTTPListenAddress).
		Envar("PLUMBER_RELAY_HTTP_LISTEN_ADDRESS").
		StringVar(&opts.Relay.HTTPListenAddress)

	relayCmd.Flag("batch-size", "How many messages to batch before sending them to grpc-collector").
		Default(DefaultCount).
		Envar("PLUMBER_RELAY_BATCH_SIZE").
		IntVar(&opts.Relay.BatchSize)
}

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
	DefaultGRPCAddress       = "grpc-collector.batch.sh:9000"
	DefaultHTTPListenAddress = ":8080"
	DefaultGRPCTimeout       = "10s"
	DefaultNumWorkers        = "10"
)

var (
	version = "UNSET"
)

type Options struct {
	// Global
	Debug   bool
	Quiet   bool
	Action  string
	Version string

	// Relay
	RelayToken             string
	RelayGRPCAddress       string
	RelayType              string
	RelayHTTPListenAddress string
	RelayNumWorkers        int
	RelayGRPCTimeout       time.Duration
	RelayGRPCDisableTLS    bool

	Kafka     *KafkaOptions
	Rabbit    *RabbitOptions
	GCPPubSub *GCPPubSubOptions
	MQTT      *MQTTOptions
	AWSSQS    *AWSSQSOptions
}

func Handle() (string, *Options, error) {
	opts := &Options{
		Kafka:     &KafkaOptions{},
		Rabbit:    &RabbitOptions{},
		GCPPubSub: &GCPPubSubOptions{},
		MQTT:      &MQTTOptions{},
		AWSSQS:    &AWSSQSOptions{WriteAttributes: make(map[string]string, 0)},
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

	// Specific actions
	readCmd := app.Command("read", "Read message(s) from messaging system")
	writeCmd := app.Command("write", "Write message(s) to messaging system")
	relayCmd := app.Command("relay", "Relay message(s) from messaging system to Batch")

	HandleRelayFlags(relayCmd, opts)
	HandleKafkaFlags(readCmd, writeCmd, opts)
	HandleRabbitFlags(readCmd, writeCmd, relayCmd, opts)
	HandleGCPPubSubFlags(readCmd, writeCmd, opts)
	HandleMQTTFlags(readCmd, writeCmd, opts)
	HandleAWSSQSFlags(readCmd, writeCmd, relayCmd, opts)

	app.Version(version)
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	cmd, err := app.Parse(os.Args[1:])
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

func HandleRelayFlags(relayCmd *kingpin.CmdClause, opts *Options) {
	relayCmd.Flag("type", "Type of collector to use. Ex: rabbit, kafka, aws-sqs").
		Envar("PLUMBER_RRELAYELAY_TYPE").
		EnumVar(&opts.RelayType, "aws-sqs", "rabbit")

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

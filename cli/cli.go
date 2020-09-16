package cli

import (
	"os"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	DefaultGRPCAddress      = "grpc-collector.batch.sh:9000"
	DefaultAPIListenAddress = ":8080"
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
	Token            string
	GRPCAddress      string
	RelayType        string
	APIListenAddress string

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
	app.Flag("debug", "Enable debug output").Short('d').BoolVar(&opts.Debug)
	app.Flag("quiet", "Suppress non-essential output").Short('q').BoolVar(&opts.Quiet)

	// Specific actions
	readCmd := app.Command("read", "Read message(s) from messaging system")
	writeCmd := app.Command("write", "Write message(s) to messaging system")
	relayCmd := app.Command("relay", "Relay message(s) from messaging system to Batch")

	HandleKafkaFlags(readCmd, writeCmd, opts)
	HandleRabbitFlags(readCmd, writeCmd, opts)
	HandleGCPPubSubFlags(readCmd, writeCmd, opts)
	HandleMQTTFlags(readCmd, writeCmd, opts)
	HandleAWSSQSFlags(readCmd, writeCmd, relayCmd, opts)
	HandleRelayFlags(relayCmd, opts)

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
	relayCmd.Flag("type", "Type of collector to use. Ex: rabbit, kafka, gcp-pubsub").
		Envar("PLUMBER_RELAY_TYPE").EnumVar(&opts.RelayType, "aws-sqs")
	relayCmd.Flag("token", "Collection token to use when sending data to Batch").
		Required().Envar("PLUMBER_TOKEN").StringVar(&opts.Token)
	relayCmd.Flag("grpc-address", "Alternative gRPC collector address").
		Default(DefaultGRPCAddress).Envar("PLUMBER_GRPC_ADDRESS").
		StringVar(&opts.GRPCAddress)
}

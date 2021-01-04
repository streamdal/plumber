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
	Address            string
	Topic              string
	Timeout            time.Duration
	InsecureTLS        bool
	Username           string
	Password           string
	AuthenticationType string

	// Read
	ReadGroupId string

	// Write
	WriteKey string
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
	cmd.Flag("username", "SASL Username").StringVar(&opts.Kafka.Username)
	cmd.Flag("password", "SASL Password. If omitted, you will be prompted for the password").StringVar(&opts.Kafka.Password)
	cmd.Flag("auth-type", "SASL Authentication type (plain or scram)").Default("scram").StringVar(&opts.Kafka.AuthenticationType)

}

func addReadKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("group-id", "Specify a specific group-id to use when reading from kafka").
		Default(KafkaDefaultGroupId).StringVar(&opts.Kafka.ReadGroupId)
}

func addWriteKafkaFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("key", "Key to write to kafka (not required)").StringVar(&opts.Kafka.WriteKey)
}

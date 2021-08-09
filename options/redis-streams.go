package options

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type RedisStreamsOptions struct {
	// Shared
	Address  string
	Username string
	Password string
	Database int

	// Read/Relay
	Streams               []string
	ConsumerGroup         string
	ConsumerName          string
	Count                 int64
	StartID               string
	RecreateConsumerGroup bool
	CreateStreams         bool

	// Write
	WriteID  string
	WriteKey string
}

func HandleRedisStreamsFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("redis-streams", "RedisPubSub Streams")
	addSharedRedisStreamsFlags(rc, opts)
	addReadRedisStreamsFlags(rc, opts)

	wc := writeCmd.Command("redis-streams", "RedisPubSub Streams")
	addSharedRedisStreamsFlags(wc, opts)
	addWriteRedisStreamsFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("redis-streams", "RedisPubSub Streams")
	}

	addSharedRedisStreamsFlags(rec, opts)
	addReadRedisStreamsFlags(rec, opts)
}

func addReadRedisStreamsFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("consumer-group", "Consumer group name").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_CONSUMER_GROUP").
		Default("plumber").
		StringVar(&opts.RedisStreams.ConsumerGroup)

	cmd.Flag("consumer-name", "Consumer name").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_CONSUMER_NAME").
		Default("plumber-consumer-1").
		StringVar(&opts.RedisStreams.ConsumerName)

	cmd.Flag("count", "Number of records to read from stream(s) per read").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_COUNT").
		Default(DefaultCount).
		Int64Var(&opts.RedisStreams.Count)

	cmd.Flag("start-id", "What id a new consumer group should start consuming messages at "+
		"(\"0\" = oldest, \"$\" = latest)").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_START_ID").
		Default("0").
		StringVar(&opts.RedisStreams.StartID)

	cmd.Flag("recreate-consumer-group", "Recreate consumer group").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_RECREATE_CONSUMER_GROUP").
		Default("false").
		BoolVar(&opts.RedisStreams.RecreateConsumerGroup)

	cmd.Flag("create-streams", "Create streams when declaring consumer group").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_CREATE_STREAMS").
		Default("false").
		BoolVar(&opts.RedisStreams.CreateStreams)
}

func addWriteRedisStreamsFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("id", "What redis ID to use for input data (\"*\" = auto-generate)").
		Default("*").
		StringVar(&opts.RedisStreams.WriteID)

	cmd.Flag("key", "Key name to write input data to").
		Required().
		StringVar(&opts.RedisStreams.WriteKey)
}

func addSharedRedisStreamsFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Address of RedisPubSub server").
		Default("localhost:6379").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_ADDRESS").
		StringVar(&opts.RedisStreams.Address)

	cmd.Flag("streams", "Streams to read/write/relay to/from").
		Required().
		Envar("PLUMBER_RELAY_REDIS_STREAMS_STREAMS").
		StringsVar(&opts.RedisStreams.Streams)

	cmd.Flag("username", "Username (redis >= v6.0.0)").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_USERNAME").
		StringVar(&opts.RedisStreams.Username)

	cmd.Flag("password", "Password (redis >= 1.0.0)").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_PASSWORD").
		StringVar(&opts.RedisStreams.Password)

	cmd.Flag("database", "Database (0-16)").
		Envar("PLUMBER_RELAY_REDIS_STREAMS_DATABASE").
		IntVar(&opts.RedisStreams.Database)
}

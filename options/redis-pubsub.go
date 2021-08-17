package options

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type RedisPubSubOptions struct {
	// Shared
	Address  string
	Channels []string
	Username string
	Password string
	Database int
}

func HandleRedisPubSubFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("redis-pubsub", "RedisPubSub Pub/Sub")
	addSharedRedisPubSubFlags(rc, opts)

	wc := writeCmd.Command("redis-pubsub", "RedisPubSub Pub/Sub")
	addSharedRedisPubSubFlags(wc, opts)

	rec := relayCmd.Command("redis-pubsub", "RedisPubSub Pub/Sub")
	addSharedRedisPubSubFlags(rec, opts)
}

func addSharedRedisPubSubFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Address of RedisPubSub server").
		Default("localhost:6379").
		Envar("PLUMBER_RELAY_REDIS_PUBSUB_ADDRESS").
		StringVar(&opts.RedisPubSub.Address)

	cmd.Flag("channels", "OutputChannel(s) to read/write/relay to/from").
		Required().
		Envar("PLUMBER_RELAY_REDIS_PUBSUB_CHANNELS").
		StringsVar(&opts.RedisPubSub.Channels)

	cmd.Flag("username", "Username (redis >= v6.0.0)").
		Envar("PLUMBER_RELAY_REDIS_PUBSUB_USERNAME").
		StringVar(&opts.RedisPubSub.Username)

	cmd.Flag("password", "Password (redis >= 1.0.0)").
		Envar("PLUMBER_RELAY_REDIS_PUBSUB_PASSWORD").
		StringVar(&opts.RedisPubSub.Password)

	cmd.Flag("database", "Database (0-16)").
		Envar("PLUMBER_RELAY_REDIS_PUBSUB_DATABASE").
		IntVar(&opts.RedisPubSub.Database)
}

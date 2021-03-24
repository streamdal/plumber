package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type RedisOptions struct {
	// Shared
	Address  string
	Channels []string
	Username string
	Password string
	Database int
}

func HandleRedisFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("redis", "Redis Pub/Sub")
	addSharedRedisFlags(rc, opts)

	wc := writeCmd.Command("redis", "Redis Pub/Sub")
	addSharedRedisFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("redis", "Redis Pub/Sub")
	}

	addSharedRedisFlags(rec, opts)
}

func addSharedRedisFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Address of Redis Server").
		Default("localhost:6379").
		Envar("PLUMBER_RELAY_REDIS_ADDRESS").
		StringVar(&opts.Redis.Address)

	cmd.Flag("channels", "Channel(s) to read/write/relay to/from").
		Required().
		Envar("PLUMBER_RELAY_REDIS_CHANNELS").
		StringsVar(&opts.Redis.Channels)

	cmd.Flag("username", "Username (redis >= v6.0.0)").
		Envar("PLUMBER_RELAY_REDIS_USERNAME").
		StringVar(&opts.Redis.Username)

	cmd.Flag("password", "Password (redis >= 1.0.0)").
		Envar("PLUMBER_RELAY_REDIS_PASSWORD").
		StringVar(&opts.Redis.Password)

	cmd.Flag("database", "Database (0-16)").
		Envar("PLUMBER_RELAY_REDIS_DATABASE").
		IntVar(&opts.Redis.Database)
}

package cli

import "gopkg.in/alecthomas/kingpin.v2"

type RedisOptions struct {
	// Shared
	Address string
	Channel string
}

func HandleRedisFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("redis", "Redis Pub/Sub")
	addSharedRedisFlags(rc, opts)

	wc := writeCmd.Command("redis", "Redis Pub/Sub")
	addSharedRedisFlags(wc, opts)
}

func addSharedRedisFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Address of Redis Server").
		Default("localhost:6379").
		StringVar(&opts.Redis.Address)
	cmd.Flag("channel", "Channel to read/write to").
		Required().
		StringVar(&opts.Redis.Channel)
}

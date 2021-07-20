package cli

import "gopkg.in/alecthomas/kingpin.v2"

type ServerOptions struct {
	ListenAddress string
	AuthToken     string
}

func HandleServerFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Listen address and port. Ex: localhost:8080").
		Default("localhost:9090").
		StringVar(&opts.Server.ListenAddress)

	cmd.Flag("token", "Plumber authentication token").
		Default("batchcorp").
		StringVar(&opts.Server.AuthToken)
}

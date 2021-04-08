package cli

import "gopkg.in/alecthomas/kingpin.v2"

type NatsOptions struct {
	// Shared
	Subject string
	Address string

	// TLS-related pieces
	TLSCAFile         string
	TLSClientCertFile string
	TLSClientKeyFile  string
	InsecureTLS       bool

	// Authentication
	CredsFile string
}

func HandleNatsFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("nats", "NATS Messaging System")
	addSharedNatsFlags(rc, opts)

	wc := writeCmd.Command("nats", "NATS Messaging System")
	addSharedNatsFlags(wc, opts)
}

func addSharedNatsFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Dial string for NATS server. Ex: nats://localhost:4222").
		StringVar(&opts.Nats.Address)
	cmd.Flag("subject", "NATS Subject. Ex: \"foo.bar.*\"").
		StringVar(&opts.Nats.Subject)
	cmd.Flag("tls-ca-file", "CA file (only needed if addr is tls://)").
		ExistingFileVar(&opts.Nats.TLSCAFile)
	cmd.Flag("tls-client-cert-file", "Client cert file (only needed if addr is tls://)").
		ExistingFileVar(&opts.Nats.TLSClientCertFile)
	cmd.Flag("tls-client-key-file", "Client key file (only needed if addr is tls://)").
		ExistingFileVar(&opts.Nats.TLSClientKeyFile)
	cmd.Flag("insecure-tls", "Whether to verify server certificate").Default("false").
		BoolVar(&opts.Nats.InsecureTLS)
	cmd.Flag("creds-file", "NATS .creds file containing authentication credentials").
		ExistingFileVar(&opts.Nats.CredsFile)
}

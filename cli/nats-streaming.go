package cli

import "gopkg.in/alecthomas/kingpin.v2"

type NatsStreamingOptions struct {
	// Shared
	Channel   string
	Address   string
	ClusterID string
	ClientID  string

	// TLS-related pieces
	TLSCAFile         string
	TLSClientCertFile string
	TLSClientKeyFile  string
	InsecureTLS       bool

	// Authentication
	CredsFile string

	// Read
	StartReadingFrom    uint64
	AllAvailable        bool
	DurableSubscription string
}

func HandleNatsStreamingFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("nats-streaming", "NATS Streaming")
	addSharedNatsStreamingFlags(rc, opts)
	addReadNatsStreamingFlags(rc, opts)

	wc := writeCmd.Command("nats-streaming", "NATS Streaming")
	addSharedNatsStreamingFlags(wc, opts)
}

func addReadNatsStreamingFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("--all-available", "Returns all available").
		BoolVar(&opts.NatsStreaming.AllAvailable)

	cmd.Flag("from-sequence", "Start reading from this sequence number").
		Uint64Var(&opts.NatsStreaming.StartReadingFrom)

	cmd.Flag("durable-name", "Create a durable subscription with this name for the given channel").
		StringVar(&opts.NatsStreaming.DurableSubscription)
}

func addSharedNatsStreamingFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Dial string for NATS server. Ex: nats://localhost:4222").
		StringVar(&opts.NatsStreaming.Address)

	cmd.Flag("cluster-id", "Cluster ID of the Nats Server").
		Required().
		StringVar(&opts.NatsStreaming.ClusterID)

	cmd.Flag("client-id", "User specified client ID to connect with").
		Required().
		StringVar(&opts.NatsStreaming.ClientID)

	cmd.Flag("channel", "NATS stream channel. Ex: \"orders\"").
		StringVar(&opts.NatsStreaming.Channel)

	cmd.Flag("tls-ca-file", "CA file (only needed if addr is tls://").ExistingFileVar(&opts.NatsStreaming.TLSCAFile)

	cmd.Flag("tls-client-cert-file", "Client cert file (only needed if addr is tls://").
		ExistingFileVar(&opts.NatsStreaming.TLSClientCertFile)

	cmd.Flag("tls-client-key-file", "Client key file (only needed if addr is tls://").
		ExistingFileVar(&opts.NatsStreaming.TLSClientKeyFile)

	cmd.Flag("insecure-tls", "Whether to verify server certificate").Default("false").
		BoolVar(&opts.NatsStreaming.InsecureTLS)

	cmd.Flag("creds-file", "NATS .creds file containing authentication credentials").
		ExistingFileVar(&opts.NatsStreaming.CredsFile)
}

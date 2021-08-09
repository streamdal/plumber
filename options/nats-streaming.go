package options

import (
	"gopkg.in/alecthomas/kingpin.v2"
	"time"
)

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
	DurableSubscription string
	ReadLastReceived    bool
	ReadSince           time.Duration
	ReadFromSequence    uint64
	AllAvailable        bool
}

func HandleNatsStreamingFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("nats-streaming", "NATS Streaming")
	addSharedNatsStreamingFlags(rc, opts)
	addReadNatsStreamingFlags(rc, opts)

	wc := writeCmd.Command("nats-streaming", "NATS Streaming")
	addSharedNatsStreamingFlags(wc, opts)
}

func addReadNatsStreamingFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("all", "Deliver all available messages").
		BoolVar(&opts.NatsStreaming.AllAvailable)

	cmd.Flag("seq", "Deliver messages starting at sequence number").
		Uint64Var(&opts.NatsStreaming.ReadFromSequence)

	cmd.Flag("since", "Deliver messages in last interval (e.g. 1s, 1h)").
		DurationVar(&opts.NatsStreaming.ReadSince)

	cmd.Flag("last", "Deliver starting with last published message").
		BoolVar(&opts.NatsStreaming.ReadLastReceived)

	cmd.Flag("durable-name", "Create a durable subscription with this name for the given channel").
		StringVar(&opts.NatsStreaming.DurableSubscription)
}

func addSharedNatsStreamingFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Dial string for NATS server. Ex: nats://localhost:4222").
		StringVar(&opts.NatsStreaming.Address)

	cmd.Flag("cluster-id", "Cluster ID of the Nats server").
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

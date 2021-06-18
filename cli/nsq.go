package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type NSQOptions struct {
	// Shared
	Topic             string
	NSQDAddress       string
	AuthSecret        string
	ClientID          string
	InsecureTLS       bool
	TLSClientKeyFile  string
	TLSClientCertFile string
	TLSCAFile         string

	// Read
	NSQLookupDAddress string
	Channel           string
}

// tls_v1 - Bool enable TLS negotiation
// tls_root_ca_file - String path to file containing root CA
// tls_insecure_skip_verify - Bool indicates whether this client should verify server certificates
// tls_cert - String path to file containing public key for certificate
// tls_key - String path to file containing private key for certificate

func HandleNSQFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("nsq", "NSQ Messaging System")
	addSharedNSQFlags(rc, opts)
	addReadNSQFlags(rc, opts)

	wc := writeCmd.Command("nsq", "NSQ Messaging System")
	addSharedNSQFlags(wc, opts)
	addWriteNSQFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("nsq", "NSQ Messaging System")
	}

	addSharedNSQFlags(rec, opts)
}

func addSharedNSQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic", "NSQ Topic to read from or write to").
		Required().
		StringVar(&opts.NSQ.Topic)

	cmd.Flag("auth-secret", "Authentication Secret").
		StringVar(&opts.NSQ.AuthSecret)

	cmd.Flag("client-id", "Client ID to identify as").
		Default("plumber").
		StringVar(&opts.NSQ.ClientID)

	cmd.Flag("tls-ca-file", "CA file (only needed if addr is ssl://").
		Envar("PLUMBER_RELAY_NSQ_TLS_CA_FILE").
		ExistingFileVar(&opts.NSQ.TLSCAFile)

	cmd.Flag("tls-client-cert-file", "Client cert file (only needed if addr is ssl://").
		Envar("PLUMBER_RELAY_NSQ_TLS_CERT_FILE").
		ExistingFileVar(&opts.NSQ.TLSClientCertFile)

	cmd.Flag("tls-client-key-file", "Client key file (only needed if addr is ssl://").
		Envar("PLUMBER_RELAY_NSQ_TLS_KEY_FILE").
		ExistingFileVar(&opts.NSQ.TLSClientKeyFile)

	cmd.Flag("insecure-tls", "Whether to verify server certificate").
		Envar("PLUMBER_RELAY_NSQ_SKIP_VERIFY_TLS").
		Default("false").
		BoolVar(&opts.NSQ.InsecureTLS)
}

func addReadNSQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("channel", "Channel").
		Required().
		StringVar(&opts.NSQ.Channel)

	cmd.Flag("lookupd-address", "Address of LookupD Server (Ex: localhost:4161)").
		StringVar(&opts.NSQ.NSQLookupDAddress)

	cmd.Flag("nsqd-address", "Address of NSQ Server (Ex: localhost:4150)").
		StringVar(&opts.NSQ.NSQDAddress)
}

func addWriteNSQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("nsqd-address", "Address of NSQ Server (Ex: localhost:4150)").
		Default("localhost:4150").
		StringVar(&opts.NSQ.NSQDAddress)
}

package options

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

// NSQOptions stores the values of CLI options for NSQ flags
type NSQOptions struct {
	// Shared
	Topic             string
	NSQDAddress       string
	AuthSecret        string
	ClientID          string
	UseTLS            bool
	InsecureTLS       bool
	TLSClientKeyFile  string
	TLSClientCertFile string
	TLSCAFile         string

	// Read
	NSQLookupDAddress string
	Channel           string
}

// HandleNSQFlags creates NSQ commands and flags
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
	addReadNSQFlags(rec, opts)
}

// addSharedNSQFlags creates flags shared between read/write/relay modes
func addSharedNSQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic", "NSQ Topic to read from or write to").
		Required().
		Envar("PLUMBER_RELAY_NSQ_TOPIC").
		StringVar(&opts.NSQ.Topic)

	cmd.Flag("auth-secret", "Authentication Secret").
		Envar("PLUMBER_RELAY_NSQ_AUTH_SECRET").
		StringVar(&opts.NSQ.AuthSecret)

	cmd.Flag("client-id", "Client ID to identify as").
		Default("plumber").
		Envar("PLUMBER_RELAY_NSQ_CLIENT_ID").
		StringVar(&opts.NSQ.ClientID)

	cmd.Flag("tls-ca-file", "CA file").
		Envar("PLUMBER_RELAY_NSQ_TLS_CA_FILE").
		ExistingFileVar(&opts.NSQ.TLSCAFile)

	cmd.Flag("tls-client-cert-file", "Client cert file").
		Envar("PLUMBER_RELAY_NSQ_TLS_CERT_FILE").
		ExistingFileVar(&opts.NSQ.TLSClientCertFile)

	cmd.Flag("tls-client-key-file", "Client key file").
		Envar("PLUMBER_RELAY_NSQ_TLS_KEY_FILE").
		ExistingFileVar(&opts.NSQ.TLSClientKeyFile)

	cmd.Flag("use-tls", "Connect securely via TLS").
		Envar("PLUMBER_RELAY_NSQ_USE_TLS").
		Default("false").
		BoolVar(&opts.NSQ.UseTLS)

	cmd.Flag("insecure-tls", "Whether to verify server certificate").
		Envar("PLUMBER_RELAY_NSQ_SKIP_VERIFY_TLS").
		Default("false").
		BoolVar(&opts.NSQ.InsecureTLS)
}

// addReadNSQFlags creates flags used for reading from NSQ
func addReadNSQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("channel", "Channel").
		Required().
		Envar("PLUMBER_RELAY_NSQ_CHANNEL").
		StringVar(&opts.NSQ.Channel)

	cmd.Flag("lookupd-address", "Address of LookupD server (Ex: localhost:4161)").
		Envar("PLUMBER_RELAY_NSQ_LOOKUPD_ADDRESS").
		StringVar(&opts.NSQ.NSQLookupDAddress)

	cmd.Flag("nsqd-address", "Address of NSQ server (Ex: localhost:4150)").
		Envar("PLUMBER_RELAY_NSQ_NSQD_ADDRESS").
		StringVar(&opts.NSQ.NSQDAddress)
}

// addWriteNSQFlags creates flags used for writing to NSQ
func addWriteNSQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("nsqd-address", "Address of NSQ server (Ex: localhost:4150)").
		Default("localhost:4150").
		StringVar(&opts.NSQ.NSQDAddress)
}

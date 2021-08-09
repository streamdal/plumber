package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

type KubeMQQueueOptions struct {
	// Shared
	Address  string
	Queue    string
	ClientID string

	// TLS
	TLSCertFile string

	// Authentication
	AuthToken string
}

func HandleKubeMQQueueFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("kubemq-queue", "KubeMQ Kubernetes Message Broker")
	addSharedKubeMQQueueFlags(rc, opts)
	wc := writeCmd.Command("kubemq-queue", "KubeMQ Kubernetes Message Broker")
	addSharedKubeMQQueueFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("kubemq-queue", "KubeMQ Kubernetes Message Broker")
	}
	addSharedKubeMQQueueFlags(rec, opts)
}

func addSharedKubeMQQueueFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Dial string for KubeMQ server. Ex: localhost:50000").
		Envar("PLUMBER_RELAY_KUBEMQ_QUEUE_ADDRESS").
		Default("localhost:50000").
		StringVar(&opts.KubeMQQueue.Address)
	cmd.Flag("queue", "KubeMQ Queue Name").
		Envar("PLUMBER_RELAY_KUBEMQ_QUEUE_QUEUE").
		StringVar(&opts.KubeMQQueue.Queue)
	cmd.Flag("client-id", "KubeMQ Client ID").
		Envar("PLUMBER_RELAY_KUBEMQ_QUEUE_CLIENT_ID").
		StringVar(&opts.KubeMQQueue.ClientID)
	cmd.Flag("tls-cert-file", "Client cert file").
		Envar("PLUMBER_RELAY_KUBEMQ_QUEUE_TLS_CERT_FILE").
		ExistingFileVar(&opts.KubeMQQueue.TLSCertFile)
	cmd.Flag("auth-token", "Client JWT Authentication Token").
		Envar("PLUMBER_RELAY_KUBEMQ_QUEUE_AUTH_TOKEN").
		StringVar(&opts.KubeMQQueue.AuthToken)
}

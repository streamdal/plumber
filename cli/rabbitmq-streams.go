package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type RabbitMQStreamsOptions struct {
	// Shared
	Address           string
	Port              int
	Stream            string
	UseTLS            bool
	SkipVerifyTLS     bool
	ClientName        string
	DeclareStream     bool
	DeclareStreamSize string
	Username          string
	Password          string

	// Read
	Offset string
}

func HandleRabbitStreamsFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	// RabbitMQ read cmd
	rc := readCmd.Command("rabbit-streams", "RabbitMQ Streams")

	addSharedRabbitMQStreamsFlags(rc, opts)
	addReadRabbitMQStreamsFlags(rc, opts)

	// Rabbit write cmd
	wc := writeCmd.Command("rabbit-streams", "RabbitMQ Streams")

	addSharedRabbitMQStreamsFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("rabbit-streams", "RabbitMQ")
	}

	addReadRabbitMQStreamsFlags(rec, opts)
	addSharedRabbitMQStreamsFlags(rec, opts)
}

func addSharedRabbitMQStreamsFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Address of RabbitMQ Server").
		Default("localhost").
		StringVar(&opts.RabbitMQStreams.Address)

	cmd.Flag("port", "RabbitMQ Streams Server Port").
		Default("5552").
		IntVar(&opts.RabbitMQStreams.Port)

	cmd.Flag("stream", "Stream Name").
		Required().
		StringVar(&opts.RabbitMQStreams.Stream)

	cmd.Flag("client-name", "Consumer or producer name to identify as to RabbitMQ").
		Default("plumber").
		StringVar(&opts.RabbitMQStreams.ClientName)

	cmd.Flag("declare-stream", "Declare the stream if it does not exist").
		BoolVar(&opts.RabbitMQStreams.DeclareStream)

	cmd.Flag("declare-stream-size", "Size capacity to declare the stream with. Ex: 10mb, 3gb, 1024kb").
		StringVar(&opts.RabbitMQStreams.DeclareStreamSize)

	cmd.Flag("username", "Username to authenticate to RabbitMQ With").
		Default("guest").
		StringVar(&opts.RabbitMQStreams.Username)

	cmd.Flag("password", "Password to authenticate to RabbitMQ With").
		Default("guest").
		StringVar(&opts.RabbitMQStreams.Password)
}

func addReadRabbitMQStreamsFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("offset", "Offset to start reading at. Choices: first, last, last-consumed, next, or a specific offset number").
		Default("next").
		StringVar(&opts.RabbitMQStreams.Offset)
}

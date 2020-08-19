package cli

import (
	"os"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version = "UNSET"
)

type Options struct {
	// Global
	Debug  bool
	Quiet  bool
	Action string

	Kafka     *KafkaOptions
	Rabbit    *RabbitOptions
	GCPPubSub *GCPPubSubOptions
	MQTT      *MQTTOptions
}

func Handle() (string, *Options, error) {
	opts := &Options{
		Kafka:     &KafkaOptions{},
		Rabbit:    &RabbitOptions{},
		GCPPubSub: &GCPPubSubOptions{},
		MQTT:      &MQTTOptions{},
	}

	app := kingpin.New("plumber", "`curl` for messaging systems. See: https://github.com/batchcorp/plumber")

	// Global
	app.Flag("debug", "Enable debug output").Short('d').BoolVar(&opts.Debug)
	app.Flag("quiet", "Suppress non-essential output").Short('q').BoolVar(&opts.Quiet)

	// Read cmd
	readCmd := app.
		Command("read", "Read message(s) from messaging system").
		Command("message", "What to read off of messaging systems").Alias("messages")

	// Write cmd
	writeCmd := app.
		Command("write", "Write message(s) to messaging system").
		Command("message", "What to write to messaging system").Alias("messages")

	HandleKafkaFlags(readCmd, writeCmd, opts)
	HandleRabbitFlags(readCmd, writeCmd, opts)
	HandleGCPPubSubFlags(readCmd, writeCmd, opts)
	HandleMQTTFlags(readCmd, writeCmd, opts)

	app.Version(version)
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return "", nil, errors.Wrap(err, "unable to parse command")
	}

	opts.Action = "unknown"

	cmds := strings.Split(cmd, " ")
	if len(cmds) > 0 {
		opts.Action = cmds[0]
	}

	return cmd, opts, err
}

package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version = "UNSET"
)

type Options struct {
	// Global
	Debug *bool
	Quiet *bool

	Kafka     *KafkaOptions
	Rabbit    *RabbitOptions
	GCPPubSub *GCPPubSubOptions
}

func Handle() (*kingpin.ParseContext, *Options, error) {
	opts := &Options{
		Kafka:     &KafkaOptions{},
		Rabbit:    &RabbitOptions{},
		GCPPubSub: &GCPPubSubOptions{},
	}

	app := kingpin.New("plumber", "`curl` for messaging systems. See: https://github.com/batchcorp/plumber")

	// Global
	app.Flag("debug", "Enable debug output").Short('d').BoolVar(opts.Debug)
	app.Flag("quiet", "Suppress non-essential output").Short('q').BoolVar(opts.Quiet)

	HandleKafkaFlags(app, opts)
	HandleRabbitFlags(app, opts)
	HandleGCPPubSubFlags(app, opts)

	app.Version(version)
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	ctx, err := app.ParseContext(os.Args[1:])

	return ctx, opts, err
}

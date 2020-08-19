package main

import (
	"github.com/sirupsen/logrus"

	gcppubsub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/cli"
)

func main() {
	cmd, opts, err := cli.Handle()
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	if opts.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	switch cmd {
	// Read
	case "read message rabbit":
		err = rabbitmq.Read(opts)
	case "read message kafka":
		err = kafka.Read(opts)
	case "read message gcp-pubsub":
		err = gcppubsub.Read(opts)
	case "read message mqtt":
		err = mqtt.Read(opts)

	// Write
	case "write message rabbit":
		err = rabbitmq.Write(opts)
	case "write message kafka":
		err = kafka.Write(opts)
	case "write message gcp-pubsub":
		err = gcppubsub.Write(opts)
	case "write message mqtt":
		err = mqtt.Write(opts)

	default:
		logrus.Fatalf("Unrecognized command: %s", cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

package main

import (
	"github.com/sirupsen/logrus"

	gcppubsub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/cli"
)

func main() {
	cmd, opts, err := cli.Handle()
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	switch cmd {
	// Read
	case "read message rabbit":
		//logrus.Fatal("read rabbit, ", opts)
		err = rabbitmq.Read(opts)
	case "read message kafka":
		//logrus.Fatalf("read kafka, ", opts)
		err = kafka.Read(opts)
	case "read message gcp-pubsub":
		//logrus.Fatalf("read gcp-pubsub, ", opts)
		err = gcppubsub.Read(opts)

	// Write
	case "write message rabbit":
		//logrus.Fatalf("write rabbit, ", opts)
		err = rabbitmq.Write(opts)
	case "write message kafka":
		//logrus.Fatalf("write kafka, ", opts)
		err = kafka.Write(opts)
	case "write message gcp-pubsub":
		//logrus.Fatalf("write gcp-pubsub, ", opts)
		err = gcppubsub.Write(opts)

	default:
		logrus.Fatalf("Unrecognized command: %s", cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/activemq"
	awssqs "github.com/batchcorp/plumber/backends/aws-sqs"
	gcppubsub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/cli"
)

func main() {
	cmd, opts, err := cli.Handle(os.Args[1:])
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	if opts.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	switch cmd {
	// Read
	case "read rabbit":
		err = rabbitmq.Read(opts)
	case "read kafka":
		err = kafka.Read(opts)
	case "read gcp-pubsub":
		err = gcppubsub.Read(opts)
	case "read mqtt":
		err = mqtt.Read(opts)
	case "read aws-sqs":
		err = awssqs.Read(opts)
	case "read activemq":
		err = activemq.Read(opts)

	// Write
	case "write rabbit":
		err = rabbitmq.Write(opts)
	case "write kafka":
		err = kafka.Write(opts)
	case "write gcp-pubsub":
		err = gcppubsub.Write(opts)
	case "write mqtt":
		err = mqtt.Write(opts)
	case "write aws-sqs":
		err = awssqs.Write(opts)
	case "write activemq":
		err = activemq.Write(opts)

	// Relay (via CLI flags)
	case "relay rabbit":
		err = rabbitmq.Relay(opts)
	case "relay kafka":
		err = kafka.Relay(opts)
	case "relay gcp-pubsub":
		err = gcppubsub.Relay(opts)
	case "relay mqtt":
		err = mqtt.Relay(opts)
	case "relay aws-sqs":
		err = awssqs.Relay(opts)

	// Relay (via env vars)
	case "relay":
		err = ProcessRelayFlags(opts)

	default:
		logrus.Fatalf("Unrecognized command: %s", cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func ProcessRelayFlags(opts *cli.Options) error {
	var err error

	switch opts.RelayType {
	case "kafka":
		err = kafka.Relay(opts)
	case "gcp-pubsub":
		err = gcppubsub.Relay(opts)
	case "mqtt":
		err = mqtt.Relay(opts)
	case "aws-sqs":
		err = awssqs.Relay(opts)
	case "rabbit":
		err = rabbitmq.Relay(opts)
	default:
		err = fmt.Errorf("unsupported messaging system '%s'", opts.RelayType)
	}

	return err
}

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/activemq"
	awssns "github.com/batchcorp/plumber/backends/aws-sns"
	awssqs "github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	gcppubsub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/backends/redis"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/stats"
)

func main() {
	cmd, opts, err := cli.Handle(os.Args[1:])
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	if opts.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if opts.Quiet {
		logrus.SetLevel(logrus.ErrorLevel)
	}

	if opts.Stats {
		stats.Start(opts.StatsReportInterval)
	}

	printer.PrintLogo()

	if strings.HasPrefix(cmd, "relay") {
		printer.PrintRelayOptions(cmd, opts)
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
	case "read azure":
		err = azure.Read(opts)
	case "read nats":
		err = nats.Read(opts)
	case "read redis":
		err = redis.Read(opts)

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
	case "write aws-sns":
		err = awssns.Write(opts)
	case "write azure":
		err = azure.Write(opts)
	case "write nats":
		err = nats.Write(opts)
	case "write redis":
		err = redis.Write(opts)

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
	case "relay azure":
		err = azure.Relay(opts)
	case "relay redis":
		err = redis.Relay(opts)

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
	case "azure":
		err = azure.Relay(opts)
	case "redis":
		err = redis.Relay(opts)
	default:
		err = fmt.Errorf("unsupported messaging system '%s'", opts.RelayType)
	}

	return err
}

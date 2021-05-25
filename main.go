package main

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/activemq"
	awssns "github.com/batchcorp/plumber/backends/aws-sns"
	awssqs "github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	azureEventhub "github.com/batchcorp/plumber/backends/azure-eventhub"
	"github.com/batchcorp/plumber/backends/batch"
	cdcMongo "github.com/batchcorp/plumber/backends/cdc-mongo"
	cdcPostgres "github.com/batchcorp/plumber/backends/cdc-postgres"
	gcppubsub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats"
	natsStreaming "github.com/batchcorp/plumber/backends/nats-streaming"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/backends/rpubsub"
	"github.com/batchcorp/plumber/backends/rstreams"
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

	// In container mode, force JSON and don't print logo
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		printer.PrintLogo()
	}

	if strings.HasPrefix(cmd, "relay") {
		printer.PrintRelayOptions(cmd, opts)
	}

	if strings.HasPrefix(cmd, "batch") {
		parseBatchCmd(cmd, opts)
		return
	}

	parseCmd(cmd, opts)
}

func parseCmd(cmd string, opts *cli.Options) {
	var err error

	switch {
	case strings.HasPrefix(cmd, "read"):
		err = parseCmdRead(cmd, opts)
	case strings.HasPrefix(cmd, "write"):
		err = parseCmdWrite(cmd, opts)
	case strings.HasPrefix(cmd, "relay"):
		err = parseCmdRelay(cmd, opts)
	case strings.HasPrefix(cmd, "dynamic"):
		err = parseCmdDynamic(cmd, opts)
	default:
		logrus.Fatalf("unrecognized command: %s", cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func parseCmdRead(cmd string, opts *cli.Options) error {
	switch cmd {
	case "read rabbit":
		return rabbitmq.Read(opts)
	case "read kafka":
		return kafka.Read(opts)
	case "read gcp-pubsub":
		return gcppubsub.Read(opts)
	case "read mqtt":
		return mqtt.Read(opts)
	case "read aws-sqs":
		return awssqs.Read(opts)
	case "read activemq":
		return activemq.Read(opts)
	case "read azure":
		return azure.Read(opts)
	case "read azure-eventhub":
		return azureEventhub.Read(opts)
	case "read nats":
		return nats.Read(opts)
	case "read nats-streaming":
		return natsStreaming.Read(opts)
	case "read redis-pubsub":
		return rpubsub.Read(opts)
	case "read redis-streams":
		return rstreams.Read(opts)
	case "read cdc-mongo":
		return cdcMongo.Read(opts)
	case "read cdc-postgres":
		return cdcPostgres.Read(opts)
	}

	return fmt.Errorf("unrecognized command: %s", cmd)
}

func parseCmdWrite(cmd string, opts *cli.Options) error {
	switch cmd {
	case "write rabbit":
		return rabbitmq.Write(opts)
	case "write kafka":
		return kafka.Write(opts)
	case "write gcp-pubsub":
		return gcppubsub.Write(opts)
	case "write mqtt":
		return mqtt.Write(opts)
	case "write aws-sqs":
		return awssqs.Write(opts)
	case "write activemq":
		return activemq.Write(opts)
	case "write aws-sns":
		return awssns.Write(opts)
	case "write azure":
		return azure.Write(opts)
	case "write azure-eventhub":
		return azureEventhub.Write(opts)
	case "write nats":
		return nats.Write(opts)
	case "write nats-streaming":
		return natsStreaming.Write(opts)
	case "write redis-pubsub":
		return rpubsub.Write(opts)
	case "write redis-streams":
		return rstreams.Write(opts)
	}

	return fmt.Errorf("unrecognized command: %s", cmd)
}

func parseCmdRelay(cmd string, opts *cli.Options) error {
	switch cmd {
	case "relay rabbit":
		return rabbitmq.Relay(opts)
	case "relay kafka":
		return kafka.Relay(opts)
	case "relay gcp-pubsub":
		return gcppubsub.Relay(opts)
	case "relay mqtt":
		return mqtt.Relay(opts)
	case "relay aws-sqs":
		return awssqs.Relay(opts)
	case "relay azure":
		return azure.Relay(opts)
	case "relay cdc-postgres":
		return cdcPostgres.Relay(opts)
	case "relay cdc-mongo":
		return cdcMongo.Relay(opts)
	case "relay redis-pubsub":
		return rpubsub.Relay(opts)
	case "relay redis-streams":
		return rstreams.Relay(opts)
		// Relay (via env vars)
	case "relay":
		return ProcessRelayFlags(opts)
	}

	return fmt.Errorf("unrecognized command: %s", cmd)
}

func parseCmdDynamic(cmd string, opts *cli.Options) error {
	parsedCmd := strings.Split(cmd, " ")
	switch parsedCmd[1] {
	case "kafka":
		return kafka.Dynamic(opts)
	case "rabbit":
		return rabbitmq.Dynamic(opts)
	case "mqtt":
		return mqtt.Dynamic(opts)
	case "redis-pubsub":
		return rpubsub.Dynamic(opts)
	case "redis-streams":
		return rstreams.Dynamic(opts)
	case "nats":
		return nats.Dynamic(opts)
	case "nats-streaming":
		return natsStreaming.Dynamic(opts)
	case "activemq":
		return activemq.Dynamic(opts)
	case "gcp-pubsub":
		return gcppubsub.Dynamic(opts)
	case "aws-sqs":
		return awssqs.Dynamic(opts)
	case "aws-sns":
		return awssns.Dynamic(opts)
	case "azure":
		return azure.Dynamic(opts)
	case "azure-eventhub":
		return azureEventhub.Dynamic(opts)
	}

	return fmt.Errorf("unrecognized command: %s", cmd)
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
	case "cdc-mongo":
		err = cdcMongo.Relay(opts)
	case "redis-pubsub":
		err = rpubsub.Relay(opts)
	case "redis-streams":
		err = rstreams.Relay(opts)
	case "cdc-postgres":
		err = cdcPostgres.Relay(opts)
	default:
		err = fmt.Errorf("unsupported messaging system '%s'", opts.RelayType)
	}

	return err
}

// parseBatchCmd handles all commands related to Batch.sh API
func parseBatchCmd(cmd string, opts *cli.Options) {
	var err error

	b := batch.New(opts)

	commands := strings.Split(cmd, " ")

	switch {
	case cmd == "batch login":
		err = b.Login()
	case cmd == "batch logout":
		err = b.Logout()
	case cmd == "batch list collection":
		err = b.ListCollections()
	case cmd == "batch create collection":
		err = b.CreateCollection()
	case cmd == "batch list destination":
		err = b.ListDestinations()
	case strings.HasPrefix(cmd, "batch create destination"):
		err = b.CreateDestination(commands[3])
	case cmd == "batch list schema":
		err = b.ListSchemas()
	case cmd == "batch list replay":
		err = b.ListReplays()
	case cmd == "batch create replay":
		err = b.CreateReplay()
	case cmd == "batch search":
		err = b.SearchCollection()
	default:
		logrus.Fatalf("unrecognized command: %s", cmd)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

package plumber

import (
	"fmt"
	"strings"

	rabbitmqStreams "github.com/batchcorp/plumber/backends/rabbitmq-streams"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/relay"

	"github.com/batchcorp/plumber/backends/activemq"
	awssns "github.com/batchcorp/plumber/backends/aws-sns"
	awssqs "github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	azureEventhub "github.com/batchcorp/plumber/backends/azure-eventhub"
	"github.com/batchcorp/plumber/backends/batch"
	cdcMongo "github.com/batchcorp/plumber/backends/cdc-mongo"
	cdcPostgres "github.com/batchcorp/plumber/backends/cdc-postgres"
	gcpPubSub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats"
	natsStreaming "github.com/batchcorp/plumber/backends/nats-streaming"
	"github.com/batchcorp/plumber/backends/nsq"
	"github.com/batchcorp/plumber/backends/pulsar"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/backends/rpubsub"
	"github.com/batchcorp/plumber/backends/rstreams"
)

// parseCmdRead handles read mode
func (p *Plumber) parseCmdRead() error {
	switch p.Cmd {
	case "read rabbit":
		return rabbitmq.Read(p.Options, p.MsgDesc)
	case "read rabbit-streams":
		return rabbitmqStreams.Read(p.Options, p.MsgDesc)
	case "read kafka":
		return kafka.Read(p.Options, p.MsgDesc)
	case "read gcp-pubsub":
		return gcpPubSub.Read(p.Options, p.MsgDesc)
	case "read mqtt":
		return mqtt.Read(p.Options, p.MsgDesc)
	case "read aws-sqs":
		return awssqs.Read(p.Options, p.MsgDesc)
	case "read activemq":
		return activemq.Read(p.Options, p.MsgDesc)
	case "read azure":
		return azure.Read(p.Options, p.MsgDesc)
	case "read azure-eventhub":
		return azureEventhub.Read(p.Options, p.MsgDesc)
	case "read nats":
		return nats.Read(p.Options, p.MsgDesc)
	case "read nats-streaming":
		return natsStreaming.Read(p.Options, p.MsgDesc)
	case "read redis-pubsub":
		return rpubsub.Read(p.Options, p.MsgDesc)
	case "read redis-streams":
		return rstreams.Read(p.Options, p.MsgDesc)
	case "read cdc-mongo":
		return cdcMongo.Read(p.Options, p.MsgDesc)
	case "read cdc-postgres":
		return cdcPostgres.Read(p.Options, p.MsgDesc)
	case "read pulsar":
		return pulsar.Read(p.Options, p.MsgDesc)
	case "read nsq":
		return nsq.Read(p.Options, p.MsgDesc)
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

// parseCmdRead handles read mode
func (p *Plumber) parseCmdLag() error {
	switch p.Cmd {
	case "lag kafka":
		return kafka.Lag(p.Options)
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

// parseCmdWrite handles write mode
func (p *Plumber) parseCmdWrite() error {
	switch p.Cmd {
	case "write rabbit":
		return rabbitmq.Write(p.Options, p.MsgDesc)
	case "write rabbit-streams":
		return rabbitmqStreams.Write(p.Options, p.MsgDesc)
	case "write kafka":
		return kafka.Write(p.Options, p.MsgDesc)
	case "write gcp-pubsub":
		return gcpPubSub.Write(p.Options, p.MsgDesc)
	case "write mqtt":
		return mqtt.Write(p.Options, p.MsgDesc)
	case "write aws-sqs":
		return awssqs.Write(p.Options, p.MsgDesc)
	case "write activemq":
		return activemq.Write(p.Options, p.MsgDesc)
	case "write aws-sns":
		return awssns.Write(p.Options, p.MsgDesc)
	case "write azure":
		return azure.Write(p.Options, p.MsgDesc)
	case "write azure-eventhub":
		return azureEventhub.Write(p.Options, p.MsgDesc)
	case "write nats":
		return nats.Write(p.Options, p.MsgDesc)
	case "write nats-streaming":
		return natsStreaming.Write(p.Options, p.MsgDesc)
	case "write redis-pubsub":
		return rpubsub.Write(p.Options, p.MsgDesc)
	case "write redis-streams":
		return rstreams.Write(p.Options, p.MsgDesc)
	case "write pulsar":
		return pulsar.Write(p.Options, p.MsgDesc)
	case "write nsq":
		return nsq.Write(p.Options, p.MsgDesc)
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

// parseCmdRelay handles CLI relay mode. Container/envar mode is handled by processRelayFlags
func (p *Plumber) parseCmdRelay() error {
	var rr relay.IRelayBackend
	var err error

	switch p.Cmd {
	case "relay rabbit":
		p.Options.RelayType = "rabbit"
		rr, err = rabbitmq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay kafka":
		p.Options.RelayType = "kafka"
		rr, err = kafka.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay gcp-pubsub":
		p.Options.RelayType = "gcp-pubsub"
		rr, err = gcpPubSub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay mqtt":
		p.Options.RelayType = "mqtt"
		rr, err = mqtt.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay aws-sqs":
		p.Options.RelayType = "aws-sqs"
		rr, err = awssqs.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay azure":
		p.Options.RelayType = "azure"
		rr, err = azure.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay cdc-postgres":
		p.Options.RelayType = "cdc-postgres"
		rr, err = cdcPostgres.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay cdc-mongo":
		p.Options.RelayType = "cdc-mongo"
		rr, err = cdcMongo.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay redis-pubsub":
		p.Options.RelayType = "redis-pubsub"
		rr, err = rpubsub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay redis-streams":
		p.Options.RelayType = "redis-streams"
		rr, err = rstreams.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay nsq":
		p.Options.RelayType = "nsq"
		rr, err = nsq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay": // Relay (via env vars)
		rr, err = p.processRelayFlags()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}

	if err != nil {
		return errors.Wrap(err, "could not instantiate relayer")
	}

	if err := p.startGRPCService(); err != nil {
		return err
	}

	if err := rr.Relay(); err != nil {
		return err
	}

	<-p.MainShutdownCtx.Done()

	p.log.Info("Application exiting")

	return nil
}

// processRelayFlags handles relay from container mode/environment variables
func (p *Plumber) processRelayFlags() (relay.IRelayBackend, error) {
	var err error
	var rr relay.IRelayBackend

	switch p.Options.RelayType {
	case "kafka":
		rr, err = kafka.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "gcp-pubsub":
		rr, err = gcpPubSub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "mqtt":
		rr, err = mqtt.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "aws-sqs":
		rr, err = awssqs.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "rabbit":
		rr, err = rabbitmq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "azure":
		rr, err = azure.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "cdc-mongo":
		rr, err = cdcMongo.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "redis-pubsub":
		rr, err = rpubsub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "redis-streams":
		rr, err = rstreams.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "cdc-postgres":
		rr, err = cdcPostgres.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "nsq":
		rr, err = nsq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	default:
		err = fmt.Errorf("unsupported messaging system '%s'", p.Options.RelayType)
	}

	return rr, err
}

// parseCmdDynamic handles dynamic replay destination mode commands
func (p *Plumber) parseCmdDynamic() error {
	parsedCmd := strings.Split(p.Cmd, " ")
	switch parsedCmd[1] {
	case "kafka":
		return kafka.Dynamic(p.Options)
	case "rabbit":
		return rabbitmq.Dynamic(p.Options)
	case "mqtt":
		return mqtt.Dynamic(p.Options)
	case "redis-pubsub":
		return rpubsub.Dynamic(p.Options)
	case "redis-streams":
		return rstreams.Dynamic(p.Options)
	case "nats":
		return nats.Dynamic(p.Options)
	case "nats-streaming":
		return natsStreaming.Dynamic(p.Options)
	case "activemq":
		return activemq.Dynamic(p.Options)
	case "gcp-pubsub":
		return gcpPubSub.Dynamic(p.Options)
	case "aws-sqs":
		return awssqs.Dynamic(p.Options)
	case "aws-sns":
		return awssns.Dynamic(p.Options)
	case "azure":
		return azure.Dynamic(p.Options)
	case "azure-eventhub":
		return azureEventhub.Dynamic(p.Options)
	}

	return fmt.Errorf("unrecognized command: %s", p.Cmd)
}

// parseBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) parseBatchCmd() error {
	b := batch.New(p.Options, p.PersistentConfig)

	switch {
	case p.Cmd == "batch login":
		return b.Login()
	case p.Cmd == "batch logout":
		return b.Logout()
	case p.Cmd == "batch list collection":
		return b.ListCollections()
	case p.Cmd == "batch create collection":
		return b.CreateCollection()
	case p.Cmd == "batch list destination":
		return b.ListDestinations()
	case strings.HasPrefix(p.Cmd, "batch create destination"):
		commands := strings.Split(p.Cmd, " ")
		return b.CreateDestination(commands[3])
	case p.Cmd == "batch list schema":
		return b.ListSchemas()
	case p.Cmd == "batch list replay":
		return b.ListReplays()
	case p.Cmd == "batch create replay":
		return b.CreateReplay()
	case p.Cmd == "batch archive replay":
		return b.ArchiveReplay()
	case p.Cmd == "batch search":
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

// parseCmdDynamic handles dynamic replay destination mode commands
func (p *Plumber) parseCmdGithub() error {
	switch {
	case p.Cmd == "github login":
		return p.githubLogin()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

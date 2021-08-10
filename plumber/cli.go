package plumber

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/backends/activemq"
	"github.com/batchcorp/plumber/backends/aws-sns"
	"github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	"github.com/batchcorp/plumber/backends/azure-eventhub"
	"github.com/batchcorp/plumber/backends/batch"
	"github.com/batchcorp/plumber/backends/cdc-mongo"
	"github.com/batchcorp/plumber/backends/cdc-postgres"
	"github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats"
	"github.com/batchcorp/plumber/backends/nats-streaming"
	"github.com/batchcorp/plumber/backends/nsq"
	"github.com/batchcorp/plumber/backends/pulsar"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/backends/rabbitmq-streams"
	"github.com/batchcorp/plumber/backends/rpubsub"
	"github.com/batchcorp/plumber/backends/rstreams"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/types"
)

// parseCmdLad handles viewing lag in CLI mode
func (p *Plumber) handleLagCmd() error {
	switch p.Cmd {
	case "lag kafka":
		return kafka.Lag(p.Options)
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

// handleRelayCmd handles CLI relay mode. Container/envar mode is handled by processRelayFlags
func (p *Plumber) handleRelayCmd() error {
	var rr relay.IRelayBackend
	var err error

	switch p.Cmd {
	case "relay rabbit":
		p.Options.Relay.Type = "rabbit"
		rr, err = rabbitmq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay kafka":
		p.Options.Relay.Type = "kafka"
		rr, err = kafka.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay gcp-pubsub":
		p.Options.Relay.Type = "gcp-pubsub"
		rr, err = gcppubsub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay mqtt":
		p.Options.Relay.Type = "mqtt"
		rr, err = mqtt.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay aws-sqs":
		p.Options.Relay.Type = "aws-sqs"
		rr, err = awssqs.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay azure":
		p.Options.Relay.Type = "azure"
		rr, err = azure.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay cdc-postgres":
		p.Options.Relay.Type = "cdc-postgres"
		rr, err = cdc_postgres.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay cdc-mongo":
		p.Options.Relay.Type = "cdc-mongo"
		rr, err = cdc_mongo.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay redis-pubsub":
		p.Options.Relay.Type = "redis-pubsub"
		rr, err = rpubsub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay redis-streams":
		p.Options.Relay.Type = "redis-streams"
		rr, err = rstreams.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay nsq":
		p.Options.Relay.Type = "nsq"
		rr, err = nsq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "relay": // Relay (via env vars)
		rr, err = p.processRelayFlags()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}

	if err != nil {
		return errors.Wrap(err, "could not instantiate relayer")
	}

	if err := p.startRelayService(); err != nil {
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

	switch p.Options.Relay.Type {
	case "kafka":
		rr, err = kafka.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "gcp-pubsub":
		rr, err = gcppubsub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "mqtt":
		rr, err = mqtt.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "aws-sqs":
		rr, err = awssqs.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "rabbit":
		rr, err = rabbitmq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "azure":
		rr, err = azure.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "cdc-mongo":
		rr, err = cdc_mongo.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "redis-pubsub":
		rr, err = rpubsub.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "redis-streams":
		rr, err = rstreams.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "cdc-postgres":
		rr, err = cdc_postgres.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	case "nsq":
		rr, err = nsq.Relay(p.Options, p.RelayCh, p.ServiceShutdownCtx)
	default:
		err = fmt.Errorf("unsupported messaging system '%s'", p.Options.Relay.Type)
	}

	return rr, err
}

// handleDynamicCmd handles dynamic replay destination mode commands
func (p *Plumber) handleDynamicCmd() error {
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
		return nats_streaming.Dynamic(p.Options)
	case "activemq":
		return activemq.Dynamic(p.Options)
	case "gcp-pubsub":
		return gcppubsub.Dynamic(p.Options)
	case "aws-sqs":
		return awssqs.Dynamic(p.Options)
	case "aws-sns":
		return awssns.Dynamic(p.Options)
	case "azure":
		return azure.Dynamic(p.Options)
	case "azure-eventhub":
		return azure_eventhub.Dynamic(p.Options)
	}

	return fmt.Errorf("unrecognized command: %s", p.Cmd)
}

// handleBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) handleBatchCmd() error {
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

// handleDynamicCmd handles dynamic replay destination mode commands
func (p *Plumber) handleGithubCmd() error {
	switch {
	case p.Cmd == "github login":
		return p.githubLogin()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}

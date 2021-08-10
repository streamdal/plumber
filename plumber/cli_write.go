package plumber

import (
	"fmt"

	"github.com/batchcorp/plumber/backends/activemq"
	"github.com/batchcorp/plumber/backends/aws-sns"
	"github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	"github.com/batchcorp/plumber/backends/azure-eventhub"
	"github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats-streaming"
	"github.com/batchcorp/plumber/backends/pulsar"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/backends/rabbitmq-streams"
	"github.com/batchcorp/plumber/backends/rpubsub"
	"github.com/batchcorp/plumber/backends/rstreams"
)

// TODO: Update
// handleWriteCmd handles write mode
func (p *Plumber) handleWriteCmd() error {
	switch p.Cmd {
	case "write rabbit":
		return rabbitmq.Write(p.Options, p.MsgDesc)
	case "write rabbit-streams":
		return rabbitmq_streams.Write(p.Options, p.MsgDesc)
	case "write kafka":
		return kafka.Write(p.Options, p.MsgDesc)
	case "write gcp-pubsub":
		return gcppubsub.Write(p.Options, p.MsgDesc)
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
		return azure_eventhub.Write(p.Options, p.MsgDesc)
	case "write nats":
		return nats.Write(p.Options, p.MsgDesc)
	case "write nats-streaming":
		return nats_streaming.Write(p.Options, p.MsgDesc)
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

package backends

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/backends/activemq"
	awssns "github.com/batchcorp/plumber/backends/aws-sns"
	awssqs "github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	azure_eventhub "github.com/batchcorp/plumber/backends/azure-eventhub"
	gcppubsub "github.com/batchcorp/plumber/backends/gcp-pubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats"
	nats_streaming "github.com/batchcorp/plumber/backends/nats-streaming"
	"github.com/batchcorp/plumber/backends/nsq"
	"github.com/batchcorp/plumber/backends/pulsar"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	rabbitmq_streams "github.com/batchcorp/plumber/backends/rabbitmq-streams"
	"github.com/batchcorp/plumber/backends/rpubsub"
	"github.com/batchcorp/plumber/backends/rstreams"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

var (
	PackageMap = map[string]func(*options.Options) (interface{}, error){}
)

// Backend is the interface that all backends implement. CLI, server, etc.
// should utilize the backends via the interface.
type Backend interface {
	Connect(ctx context.Context) error // Stop via Disconnect() or cancel context
	Disconnect(ctx context.Context) error
	Read(ctx context.Context, results chan []*types.Message) error // Stop via context
	Write(ctx context.Context, message ...interface{}) error
	Test(ctx context.Context) error
}

// New is a convenience function to instantiate the appropriate backend based on
// package name of the backend.
func New(name string, opts *options.Options) (Backend, error) {
	var be Backend
	var err error

	switch name {
	case "activemq":
		be, err = activemq.New(opts)
	case "aws-sqs":
		be, err = awssqs.New(opts)
	case "aws-sns":
		be, err = awssns.New(opts)
	case "azure":
		be, err = azure.New(opts)
	case "azure-eventhub":
		be, err = azure_eventhub.New(opts)
	case "gcp-pubsub":
		be, err = gcppubsub.New(opts)
	case "kafka":
		be, err = kafka.New(opts)
	case "mqtt":
		be, err = mqtt.New(opts)
	case "nats":
		be, err = nats.New(opts)
	case "nats-streaming":
		be, err = nats_streaming.New(opts)
	case "nsq":
		be, err = nsq.New(opts)
	case "pulsar":
		be, err = pulsar.New(opts)
	case "rabbitmq":
		be, err = rabbitmq.New(opts)
	case "rabbitmq-streams":
		be, err = rabbitmq_streams.New(opts)
	case "redis-pubsub":
		be, err = rpubsub.New(opts)
	case "redis-streams":
		be, err = rstreams.New(opts)
	default:
		return nil, fmt.Errorf("unknown backend '%s'", name)

	}

	if err != nil {
		return nil, fmt.Errorf("unable to instantiate '%s' backend: %s", name, err)
	}

	return be, nil
}

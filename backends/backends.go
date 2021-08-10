package backends

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/backends/activemq"
	"github.com/batchcorp/plumber/backends/aws-sns"
	"github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	"github.com/batchcorp/plumber/backends/azure-eventhub"
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
)

var (
	PackageMap = map[string]func(*options.Options) (Backend, error){
		"activemq":         activemq.New,
		"aws-sqs":          awssqs.New,
		"aws-sns":          awssns.New,
		"azure":            azure.New,
		"azure-eventhub":   azure_eventhub.New,
		"gcp-pubsub":       gcppubsub.New,
		"kafka":            kafka.New,
		"mqtt":             mqtt.New,
		"nats":             nats.New,
		"nats-streaming":   nats_streaming.New,
		"nsq":              nsq.New,
		"pulsar":           pulsar.New,
		"rabbitmq":         rabbitmq.New,
		"rabbitmq-streams": rabbitmq_streams.New,
		"redis-pubsub":     rpubsub.New,
		"redis-streams":    rstreams.New,
	}
)

// Backend is the interface that all backends implement. CLI, server, etc.
// should utilize the backends via the interface.
type Backend interface {
	Connect(ctx context.Context, opts *options.Options) error
	Disconnect(ctx context.Context) error
	Read(ctx context.Context, opts *options.Options) ([]byte, error)
	Write(ctx context.Context, opts *options.Options) error
	Test(ctx context.Context, opts *options.Options) error
}

// New is a convenience function to instantiate the appropriate backend based on
// package name of the backend.
func New(name string, opts *options.Options) (Backend, error) {
	f, ok := PackageMap[name]
	if !ok {
		return nil, fmt.Errorf("unknown backend '%s'", name)
	}

	be, err := f(opts)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate backend '%s': %s", name, err)
	}

	return be, nil
}

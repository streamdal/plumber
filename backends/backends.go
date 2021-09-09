package backends

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/activemq"
	awssns "github.com/batchcorp/plumber/backends/aws-sns"
	awssqs "github.com/batchcorp/plumber/backends/aws-sqs"
	"github.com/batchcorp/plumber/backends/azure"
	azure_eventhub "github.com/batchcorp/plumber/backends/azure-eventhub"
	cdc_mongo "github.com/batchcorp/plumber/backends/cdc-mongo"
	cdc_postgres "github.com/batchcorp/plumber/backends/cdc-postgres"
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
	"github.com/batchcorp/plumber/types"
)

// Backend is the interface that all backends implement; the interface is used
// for facilitating all CLI and server functionality in plumber.
// NOTE: Most backends do not support _some_ part of the interface - in those
// cases the methods will either return types.NotImplementedErr or
// types.UnsupportedFeatureErr.
type Backend interface {
	// Read will read data from the bus and dump each message to the results
	// channel. This method should _not_ decode the message - that is left up
	// to the upstream user. The error channel _should_ be optional.
	Read(ctx context.Context, resultsChan chan *records.Read, errorChan chan *records.Error) error

	// Write will attempt to write the input messages as a batch (if the backend
	// supports batch writing). This call will block until success/error.
	//
	// NOTE: Key, headers and any other metadata is fetched from CLIOptions
	// (that are passed when instantiating the backend).
	Write(ctx context.Context, errorCh chan *records.Error, messages ...*records.Write) error

	// Test performs a "test" to see if the connection to the backend is alive.
	// The test varies between backends (ie. in kafka, it might be just attempting
	// to connect to a broker, while with another backend, plumber might try to
	// put/get sample data).
	Test(ctx context.Context) error

	// Dynamic enables plumber to become a "dynamic replay destination".
	// This is a blocking call.
	Dynamic(ctx context.Context) error

	// Lag returns consumer lag stats. Not supported by all backends.
	Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error

	// Relay will hook into a message bus as a consumer and relay all messages
	// to the relayCh; if an error channel is provided, any errors will be piped
	// to the channel as well. This method _usually_ blocks.
	Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *records.Error) error

	// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
	DisplayMessage(msg *records.Read) error

	// DisplayError will parse an Error record and print (pretty) output to STDOUT
	DisplayError(msg *records.Error) error

	// Close closes any connections the backend has open. Once this is ran, you
	// should create a new backend instance.
	Close(ctx context.Context) error

	// Name returns the name of the backend
	Name() string
}

// New is a convenience function to instantiate the appropriate backend based on
// package name of the backend.
func New(name string, cfg *protos.ConnectionConfig) (Backend, error) {
	var be Backend
	var err error

	switch name {
	case "activemq":
		be, err = activemq.New(cfg)
	case "aws-sqs":
		be, err = awssqs.New(cfg)
	case "aws-sns":
		be, err = awssns.New(cfg)
	case "azure":
		be, err = azure.New(cfg)
	case "azure-eventhub":
		be, err = azure_eventhub.New(cfg)
	case "gcp-pubsub":
		be, err = gcppubsub.New(cfg)
	case "kafka":
		be, err = kafka.New(cfg)
	case "mqtt":
		be, err = mqtt.New(cfg)
	case "nats":
		be, err = nats.New(cfg)
	case "nats-streaming":
		be, err = nats_streaming.New(cfg)
	case "nsq":
		be, err = nsq.New(cfg)
	case "pulsar":
		be, err = pulsar.New(cfg)
	case "rabbit":
		be, err = rabbitmq.New(cfg)
	case "rabbit-streams":
		be, err = rabbitmq_streams.New(cfg)
	case "redis-pubsub":
		be, err = rpubsub.New(cfg)
	case "redis-streams":
		be, err = rstreams.New(cfg)
	case "cdc-mongo":
		be, err = cdc_mongo.New(cfg)
	case "cdc-postgres":
		be, err = cdc_postgres.New(cfg)
	default:
		return nil, fmt.Errorf("unknown backend '%s'", name)

	}

	if err != nil {
		return nil, fmt.Errorf("unable to instantiate '%s' backend: %s", name, err)
	}

	return be, nil
}

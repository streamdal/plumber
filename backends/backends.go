package backends

import (
	"context"
	"fmt"
	"time"

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
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

// Backend is the interface that all backends implement; the interface is used
// for facilitating all CLI and server functionality in plumber.
// NOTE: Most backends do not support _some_ part of the interface - in those
// cases the methods will either return types.NotImplementedErr or
// types.UnsupportedFeatureErr.
type Backend interface {
	// Close closes any connections the backend has open. Once this is ran, you
	// should create a new backend instance.
	Close(ctx context.Context) error

	// Read will read data from the bus and dump each message to the results
	// channel. This method should _not_ decode the message - that is left up
	// to the upstream user. The error channel _should_ be optional.
	Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error

	// Write will attempt to write the input messages as a batch (if the backend
	// supports batch writing). This call will block until success/error.
	//
	// NOTE: Key, headers and any other metadata is fetched from Options
	// (that are passed when instantiating the backend). Ie. If you want to
	// write data to a specific key in Kafka - you'll need to pass
	// Options.Kafka.WriteKey. This is not great but will suffice for now. :(
	Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error

	// Test performs a "test" to see if the connection to the backend is alive.
	// The test varies between backends (ie. in kafka, it might be just attempting
	// to connect to a broker, while with another backend, plumber might try to
	// put/get sample data).
	Test(ctx context.Context) error

	// Dynamic enables plumber to become a "dynamic replay destination".
	// This is a blocking call.
	Dynamic(ctx context.Context) error

	// Lag returns consumer lag stats.
	Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error

	// Relay will hook into a message bus as a consumer and relay all messages
	// to the relayCh; if an error channel is provided, any errors will be piped
	// to the channel as well. This method _usually_ blocks.
	Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error

	// DisplayMessage will parse ReadMessage and print the output to STDOUT
	DisplayMessage(msg *types.ReadMessage) error

	// DisplayError will parse ErrorMessage and print the output to STDOUT
	DisplayError(msg *types.ErrorMessage) error

	// ConvertReadToRecord converts a message that is received on the result
	// channel to a protobuf Record (that is used when plumber is in server mode)
	ConvertReadToRecord(msgID, plumberID string, readMsg *types.ReadMessage) (*records.Message, error)

	// Name returns the name of the backend
	Name() string
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
	case "rabbit":
		be, err = rabbitmq.New(opts)
	case "rabbit-streams":
		be, err = rabbitmq_streams.New(opts)
	case "redis-pubsub":
		be, err = rpubsub.New(opts)
	case "redis-streams":
		be, err = rstreams.New(opts)
	case "cdc-mongo":
		be, err = cdc_mongo.New(opts)
	case "cdc-postgres":
		be, err = cdc_postgres.New(opts)
	default:
		return nil, fmt.Errorf("unknown backend '%s'", name)

	}

	if err != nil {
		return nil, fmt.Errorf("unable to instantiate '%s' backend: %s", name, err)
	}

	return be, nil
}

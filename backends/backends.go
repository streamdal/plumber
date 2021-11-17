package backends

import (
	"context"

	"github.com/batchcorp/plumber/backends/rpubsub"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/awssns"
	"github.com/batchcorp/plumber/backends/awssqs"
	"github.com/batchcorp/plumber/backends/cdcmongo"
	"github.com/batchcorp/plumber/backends/cdcpostgres"
	"github.com/batchcorp/plumber/backends/gcppubsub"
	"github.com/batchcorp/plumber/backends/kafka"
	kubemqQueue "github.com/batchcorp/plumber/backends/kubemq-queue"
	"github.com/batchcorp/plumber/backends/mqtt"
	"github.com/batchcorp/plumber/backends/nats"
	"github.com/batchcorp/plumber/backends/nsq"
	rabbitStreams "github.com/batchcorp/plumber/backends/rabbit-streams"
	"github.com/batchcorp/plumber/backends/rabbitmq"
	"github.com/batchcorp/plumber/backends/rstreams"
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
	//
	// Decoding should happen _outside_ the backend.
	Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error

	// Write will attempt to write the input messages as a batch (if the backend
	// supports batch writing). This call will block until success/error.
	//
	// Encoding should happen _outside_ the backend.
	//
	// NOTE: Key, headers and any other metadata is fetched from CLIOptions
	// (that are passed when instantiating the backend).
	Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error

	// Test performs a "test" to see if the connection to the backend is alive.
	// The test varies between backends (ie. in kafka, it might be just attempting
	// to connect to a broker, while with another backend, plumber might try to
	// put/get sample data).
	Test(ctx context.Context) error

	// Dynamic enables plumber to become a "dynamic replay destination".
	// This is a blocking call.
	Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error

	// Relay will hook into a message bus as a consumer and relay all messages
	// to the relayCh; if an error channel is provided, any errors will be piped
	// to the channel as well. This method _usually_ blocks.
	Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error

	// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
	DisplayMessage(msg *records.ReadRecord) error

	// DisplayError will parse an Error record and print (pretty) output to STDOUT
	DisplayError(msg *records.ErrorRecord) error

	// Close closes any connections the backend has open. Once this is ran, you
	// should create a new backend instance.
	Close(ctx context.Context) error

	// Name returns the name of the backend
	Name() string
}

// New is a convenience function to instantiate the appropriate backend based on
// package name of the backend.
func New(connOpts *opts.ConnectionOptions) (Backend, error) {
	var be Backend
	var err error

	switch connOpts.Conn.(type) {
	case *opts.ConnectionOptions_Kafka:
		be, err = kafka.New(connOpts)
	//case *opts.ConnectionOptions_ActiveMq:
	//	be, err = activemq.New(connOpts)
	case *opts.ConnectionOptions_Awssqs:
		be, err = awssqs.New(connOpts)
	case *opts.ConnectionOptions_Awssns:
		be, err = awssns.New(connOpts)
	//case *opts.ConnectionOptions_AzureServiceBus:
	//	be, err = azure.New(connOpts)
	//case *opts.ConnectionOptions_AzureEventHub:
	//	be, err = azure_eventhub.New(cfg)
	case *opts.ConnectionOptions_Mqtt:
		be, err = mqtt.New(connOpts)
	case *opts.ConnectionOptions_GcpPubsub:
		be, err = gcppubsub.New(connOpts)
	//case *opts.ConnectionOptions_Mqtt:
	//	be, err = mqtt.New(connOpts)
	case *opts.ConnectionOptions_Nats:
		be, err = nats.New(connOpts)
	//case *opts.ConnectionOptions_NatsStreaming:
	//	be, err = nats_streaming.New(connOpts)
	case *opts.ConnectionOptions_Nsq:
		be, err = nsq.New(connOpts)
	//case *opts.ConnectionOptions_Pulsar:
	//	be, err = pulsar.New(connOpts)
	case *opts.ConnectionOptions_Rabbit:
		be, err = rabbitmq.New(connOpts)
	case *opts.ConnectionOptions_RabbitStreams:
		be, err = rabbitStreams.New(connOpts)
	case *opts.ConnectionOptions_RedisPubsub:
		be, err = rpubsub.New(connOpts)
	case *opts.ConnectionOptions_RedisStreams:
		be, err = rstreams.New(connOpts)
	case *opts.ConnectionOptions_Mongo:
		be, err = cdcmongo.New(connOpts)
	case *opts.ConnectionOptions_KubemqQueue:
		be, err = kubemqQueue.New(connOpts)
	case *opts.ConnectionOptions_Postgres:
		be, err = cdcpostgres.New(connOpts)
	default:
		return nil, errors.New("unknown backend")

	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate backend")
	}

	return be, nil
}

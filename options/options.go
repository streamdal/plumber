// Package options is a common options interface that is used by CLI, args,
// and the gRPC server. Its purpose is primarily to store all available options
// for plumber - its other responsibilities are to perform "light" validation.
//
// Additional validation should be performed by the utilizers of the options
// package.
package options

import (
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	version = "UNSET"
)

func New(args []string) (*kong.Context, *protos.CLIOptions, error) {
	cliOpts := newCLIOptions()

	maybeDisplayVersion(os.Args)

	k, err := kong.New(args)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create new kong instance")
	}

	kongCtx, err := k.Parse(args)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to parse CLI options")
	}

	cliOpts.Global.XAction = kongCtx.Command()
	cliOpts.Global.XFullCommand = strings.Join(args, " ")

	logrus.Infof("opts.Global.XAction: %s\n", cliOpts.Global.XAction)
	logrus.Infof("opts.Global.XFullCommand: %s\n", cliOpts.Global.XFullCommand)

	return kongCtx, cliOpts, nil
}

func maybeDisplayVersion(args []string) {
	for _, f := range args {
		if f == "--version" {
			fmt.Println(version)
			os.Exit(0)
		}
	}
}

// TODO: Implement
func GenerateFromReadReq(md *desc.MessageDescriptor, read *protos.CreateReadRequest) (*protos.ReadConfig, error) {
	// md could be nil if there were no decoding options

	return nil, nil
}

// TODO: Implement
func GenerateFromWriteReq(md *desc.MessageDescriptor, req *protos.WriteRequest) (*protos.WriteConfig, error) {
	// md could be nil, if there are no encoding options

	return nil, nil
}

// We have to do this in order to ensure that kong has valid destinations to
// write opts to.
func newCLIOptions() *protos.CLIOptions {
	return &protos.CLIOptions{
		Global: &protos.GlobalCLIOptions{},
		Server: &protos.ServerConfig{},
		Read:   newReadConfig(),
		Write:  newWriteConfig(),
		Relay:  newRelayConfig(),
	}
}

func newReadConfig() *protos.ReadConfig {
	return &protos.ReadConfig{
		SampleOptions: &protos.ReadSampleOptions{},
		DecodeOptions: &encoding.DecodeOptions{
			Metadata: make(map[string]string, 0),
		},
		XCliConfig: &protos.ReadCLIConfig{
			ConvertOutput: make([]protos.ConvertOption, 0),
		},
		ReadOpts: &opts.Read{
			Kafka: &opts.ReadOptsKafka{
				XConn: &args.KafkaConn{
					Address: make([]string, 0),
				},
				Args: &args.KafkaReadArgs{
					Topics: make([]string, 0),
				},
			},
			Activemq: &opts.ReadOptsActiveMQ{
				XConn: &args.ActiveMQConn{},
				Args:  &args.ActiveMQReadArgs{},
			},
			Awssqs: &opts.ReadOptsAWSSQS{
				XConn: &args.AWSSQSConn{},
				Args:  &args.AWSSQSReadArgs{},
			},
			Mongo: &opts.ReadOptsMongo{
				XConn: &args.MongoConn{},
				Args:  &args.MongoReadArgs{},
			},
			Nats: &opts.ReadOptsNats{
				XConn: &args.NatsConn{
					TlsCaCert:       make([]byte, 0),
					TlsClientCert:   make([]byte, 0),
					TlsClientKey:    make([]byte, 0),
					UserCredentials: make([]byte, 0),
				},
				Args: &args.NatsReadArgs{},
			},
			NatsStreaming: &opts.ReadOptsNatsStreaming{
				XConn: &args.NatsStreamingConn{
					TlsCaCert:       make([]byte, 0),
					TlsClientCert:   make([]byte, 0),
					TlsClientKey:    make([]byte, 0),
					UserCredentials: make([]byte, 0),
				},
				Args: &args.NatsStreamingReadArgs{},
			},
			Nsq: &opts.ReadOptsNSQ{
				XConn: &args.NSQConn{
					TlsCaCert:     make([]byte, 0),
					TlsClientCert: make([]byte, 0),
					TlsClientKey:  make([]byte, 0),
				},
				Args: &args.NSQReadArgs{},
			},
			Pulsar: &opts.ReadOptsPulsar{
				XConn: &args.PulsarConn{
					TlsClientCert: make([]byte, 0),
					TlsClientKey:  make([]byte, 0),
				},
				Args: &args.PulsarReadArgs{},
			},
			Rabbit: &opts.ReadOptsRabbit{
				XConn: &args.RabbitConn{},
				Args:  &args.RabbitReadArgs{},
			},
			RabbitStreams: &opts.ReadOptsRabbitStreams{
				XConn: &args.RabbitStreamsConn{},
				Args:  &args.RabbitStreamsReadArgs{},
			},
			Mqtt: &opts.ReadOptsMQTT{
				XConn: &args.MQTTConn{
					TlsOptions: &args.MQTTTLSOptions{},
				},
				Args: &args.MQTTReadArgs{},
			},
			AzureServiceBus: &opts.ReadOptsAzureServiceBus{
				XConn: &args.AzureServiceBusConn{},
				Args:  &args.AzureServiceBusReadArgs{},
			},
			AzureEventHub: &opts.ReadOptsAzureEventHub{
				XConn: &args.AzureEventHubConn{},
				Args:  &args.AzureEventHubReadArgs{},
			},
			GcpPubsub: &opts.ReadOptsGCPPubSub{
				XConn: &args.GCPPubSubConn{},
				Args:  &args.GCPPubSubReadArgs{},
			},
			KubemqQueue: &opts.ReadOptsKubeMQQueue{
				XConn: &args.KubeMQQueueConn{},
				Args:  &args.KubeMQQueueReadArgs{},
			},
			RedisPubsub: &opts.ReadOptsRedisPubSub{
				XConn: &args.RedisPubSubConn{},
				Args: &args.RedisPubSubReadArgs{
					Channel: make([]string, 0),
				},
			},
			RedisStreams: &opts.ReadOptsRedisStreams{
				XConn: &args.RedisStreamsConn{},
				Args: &args.RedisStreamsReadArgs{
					Stream:               make([]string, 0),
					CreateConsumerConfig: &args.CreateConsumerConfig{},
				},
			},
			Postgres: &opts.ReadOptsPostgres{
				XConn: &args.PostgresConn{},
				Args:  &args.PostgresReadArgs{},
			},
		},
	}
}

func newWriteConfig() *protos.WriteConfig {
	return &protos.WriteConfig{
		EncodeOptions: &encoding.EncodeOptions{
			Input:    make(map[string]string, 0),
			Metadata: make(map[string]string, 0),
		},
		WriteOpts: &opts.Write{
			Record: &records.WriteRecord{
				InputMetadata: make(map[string]string, 0),
			},
			Kafka: &opts.WriteOptsKafka{
				XConn: &args.KafkaConn{
					Address: make([]string, 0),
				},
				Args: &args.KafkaWriteArgs{},
			},
			Activemq: &opts.WriteOptsActiveMQ{
				XConn: &args.ActiveMQConn{},
				Args:  &args.ActiveMQWriteArgs{},
			},
			Awssqs: &opts.WriteOptsAWSSQS{
				XConn: &args.AWSSQSConn{},
				Args:  &args.AWSSQSWriteArgs{},
			},
			Nats: &opts.WriteOptsNats{
				XConn: &args.NatsConn{
					TlsCaCert:       make([]byte, 0),
					TlsClientCert:   make([]byte, 0),
					TlsClientKey:    make([]byte, 0),
					UserCredentials: make([]byte, 0),
				},
				Args: &args.NatsWriteArgs{},
			},
			NatsStreaming: &opts.WriteOptsNatsStreaming{
				XConn: &args.NatsStreamingConn{
					TlsCaCert:       make([]byte, 0),
					TlsClientCert:   make([]byte, 0),
					TlsClientKey:    make([]byte, 0),
					UserCredentials: make([]byte, 0),
				},
				Args: &args.NatsStreamingWriteArgs{},
			},
			Nsq: &opts.WriteOptsNSQ{
				XConn: &args.NSQConn{
					TlsCaCert:     make([]byte, 0),
					TlsClientCert: make([]byte, 0),
					TlsClientKey:  make([]byte, 0),
				},
				Args: &args.NSQWriteArgs{},
			},
			Pulsar: &opts.WriteOptsPulsar{
				XConn: &args.PulsarConn{
					TlsClientCert: make([]byte, 0),
					TlsClientKey:  make([]byte, 0),
				},
				Args: &args.PulsarWriteArgs{},
			},
			Rabbit: &opts.WriteOptsRabbit{
				XConn: &args.RabbitConn{},
				Args:  &args.RabbitWriteArgs{},
			},
			RabbitStreams: &opts.WriteOptsRabbitStreams{
				XConn: &args.RabbitStreamsConn{},
				Args:  &args.RabbitStreamsWriteArgs{},
			},
			Mqtt: &opts.WriteOptsMQTT{
				XConn: &args.MQTTConn{
					TlsOptions: &args.MQTTTLSOptions{},
				},
				Args: &args.MQTTWriteArgs{},
			},
			AzureServiceBus: &opts.WriteOptsAzureServiceBus{
				XConn: &args.AzureServiceBusConn{},
				Args:  &args.AzureServiceBusWriteArgs{},
			},
			AzureEventHub: &opts.WriteOptsAzureEventHub{
				XConn: &args.AzureEventHubConn{},
				Args:  &args.AzureEventHubWriteArgs{},
			},
			GcpPubsub: &opts.WriteOptsGCPPubSub{
				XConn: &args.GCPPubSubConn{},
				Args:  &args.GCPPubSubWriteArgs{},
			},
			KubemqQueue: &opts.WriteOptsKubeMQQueue{
				XConn: &args.KubeMQQueueConn{},
				Args:  &args.KubeMQQueueWriteArgs{},
			},
			RedisPubsub: &opts.WriteOptsRedisPubSub{
				XConn: &args.RedisPubSubConn{},
				Args:  &args.RedisPubSubWriteArgs{},
			},
			RedisStreams: &opts.WriteOptsRedisStreams{
				XConn: &args.RedisStreamsConn{},
				Args:  &args.RedisStreamsWriteArgs{},
			},
		},
		XCliConfig: &protos.WriteCLIConfig{},
	}
}

func newRelayConfig() *protos.RelayConfig {
	return &protos.RelayConfig{
		RelayOpts: &opts.Relay{
			Kafka: &opts.RelayOptsKafka{
				XConn: &args.KafkaConn{
					Address: make([]string, 0),
				},
				Args: &args.KafkaRelayArgs{
					Topics: make([]string, 0),
				},
			},
			Awssqs: &opts.RelayOptsAWSSQS{
				XConn: &args.AWSSQSConn{},
				Args:  &args.AWSSQSRelayArgs{},
			},
			Mongo: &opts.RelayOptsMongo{
				XConn: &args.MongoConn{},
				Args:  &args.MongoReadArgs{},
			},
			Nsq: &opts.RelayOptsNSQ{
				XConn: &args.NSQConn{
					TlsCaCert:     make([]byte, 0),
					TlsClientCert: make([]byte, 0),
					TlsClientKey:  make([]byte, 0),
				},
				Args: &args.NSQReadArgs{},
			},
			Rabbit: &opts.RelayOptsRabbit{
				XConn: &args.RabbitConn{},
				Args:  &args.RabbitReadArgs{},
			},
			Mqtt: &opts.RelayOptsMQTT{
				XConn: &args.MQTTConn{
					TlsOptions: &args.MQTTTLSOptions{},
				},
				Args: &args.MQTTReadArgs{},
			},
			AzureServiceBus: &opts.RelayOptsAzureServiceBus{
				XConn: &args.AzureServiceBusConn{},
				Args:  &args.AzureServiceBusReadArgs{},
			},
			GcpPubsub: &opts.RelayOptsGCPPubSub{
				XConn: &args.GCPPubSubConn{},
				Args:  &args.GCPPubSubReadArgs{},
			},
			KubemqQueue: &opts.RelayOptsKubeMQQueue{
				XConn: &args.KubeMQQueueConn{},
				Args:  &args.KubeMQQueueReadArgs{},
			},
			RedisPubsub: &opts.RelayOptsRedisPubSub{
				XConn: &args.RedisPubSubConn{},
				Args: &args.RedisPubSubReadArgs{
					Channel: make([]string, 0),
				},
			},
			RedisStreams: &opts.RelayOptsRedisStreams{
				XConn: &args.RedisStreamsConn{},
				Args: &args.RedisStreamsReadArgs{
					Stream:               make([]string, 0),
					CreateConsumerConfig: &args.CreateConsumerConfig{},
				},
			},
			Postgres: &opts.RelayOptsPostgres{
				XConn: &args.PostgresConn{},
				Args:  &args.PostgresReadArgs{},
			},
		},
	}
}

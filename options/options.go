// Package options is a common options interface that is used by CLI, backends,
// and the gRPC server. Its purpose is primarily to store all available options
// for plumber - its other responsibilities are to perform "light" validation.
//
// Additional validation should be performed by the utilizers of the options
// package.
package options

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/backends"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
)

var (
	version = "UNSET"
)

func New(args []string) (*kong.Context, *protos.CLIOptions, error) {
	opts := newOptions()

	maybeDisplayVersion(os.Args)

	k, err := kong.New(args)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create new kong instance")
	}

	kongCtx, err := k.Parse(args)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to parse CLI options")
	}

	return kongCtx, opts, nil
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
func newOptions() *protos.CLIOptions {
	return &protos.CLIOptions{
		Global: &protos.GlobalCLIOptions{},
		Read: &protos.ReadConfig{
			SampleOptions: &protos.ReadSampleOptions{},
			DecodeOptions: &encoding.DecodeOptions{
				Metadata: make(map[string]string, 0),
			},
			XCliConfig: &protos.ReadCLIConfig{
				ConvertOutput: make([]protos.ConvertOption, 0),
				XReadBackend: &protos.ReadCLIConfig_ReadBackend{
					Kafka: &protos.ReadCLIConfig_ReadBackend_Kafka{
						Conn: &backends.KafkaConn{
							Address: make([]string, 0),
						},
						Args: &backends.KafkaReadArgs{
							Topics: make([]string, 0),
						},
					},
					Activemq: &protos.ReadCLIConfig_ReadBackend_ActiveMQ{
						Conn: &backends.ActiveMQConn{},
						Args: &backends.ActiveMQReadArgs{},
					},
					Awssqs: &protos.ReadCLIConfig_ReadBackend_AWSSQS{
						Conn: &backends.AWSSQSConn{},
						Args: &backends.AWSSQSReadArgs{},
					},
					Mongo: &protos.ReadCLIConfig_ReadBackend_Mongo{
						Conn: &backends.MongoConn{},
						Args: &backends.MongoReadArgs{},
					},
					Nats: &protos.ReadCLIConfig_ReadBackend_Nats{
						Conn: &backends.NatsConn{
							TlsCaCert:       make([]byte, 0),
							TlsClientCert:   make([]byte, 0),
							TlsClientKey:    make([]byte, 0),
							UserCredentials: make([]byte, 0),
						},
						Args: &backends.NatsReadArgs{},
					},
					NatsStreaming: &protos.ReadCLIConfig_ReadBackend_NatsStreaming{
						Conn: &backends.NatsStreamingConn{
							TlsCaCert:       make([]byte, 0),
							TlsClientCert:   make([]byte, 0),
							TlsClientKey:    make([]byte, 0),
							UserCredentials: make([]byte, 0),
						},
						Args: &backends.NatsStreamingReadArgs{},
					},
					Nsq: &protos.ReadCLIConfig_ReadBackend_NSQ{
						Conn: &backends.NSQConn{
							TlsCaCert:     make([]byte, 0),
							TlsClientCert: make([]byte, 0),
							TlsClientKey:  make([]byte, 0),
						},
						Args: &backends.NSQReadArgs{},
					},
					Pulsar: &protos.ReadCLIConfig_ReadBackend_Pulsar{
						Conn: &backends.PulsarConn{
							TlsClientCert: make([]byte, 0),
							TlsClientKey:  make([]byte, 0),
						},
						Args: &backends.PulsarReadArgs{},
					},
					Rabbit: &protos.ReadCLIConfig_ReadBackend_Rabbit{
						Conn: &backends.RabbitConn{},
						Args: &backends.RabbitReadArgs{},
					},
					RabbitStreams: &protos.ReadCLIConfig_ReadBackend_RabbitStreams{
						Conn: &backends.RabbitStreamsConn{},
						Args: &backends.RabbitStreamsReadArgs{},
					},
					Mqtt: &protos.ReadCLIConfig_ReadBackend_MQTT{
						Conn: &backends.MQTTConn{
							TlsOptions: &backends.MQTTTLSOptions{},
						},
						Args: &backends.MQTTReadArgs{},
					},
					AzureServiceBus: &protos.ReadCLIConfig_ReadBackend_AzureServiceBus{
						Conn: &backends.AzureServiceBusConn{},
						Args: &backends.AzureServiceBusReadArgs{},
					},
					AzureEventHub: &protos.ReadCLIConfig_ReadBackend_AzureEventHub{
						Conn: &backends.AzureEventHubConn{},
						Args: &backends.AzureEventHubReadArgs{},
					},
					GcpPubsub: &protos.ReadCLIConfig_ReadBackend_GCPPubSub{
						Conn: &backends.GCPPubSubConn{},
						Args: &backends.GCPPubSubReadArgs{},
					},
					KubemqQueue: &protos.ReadCLIConfig_ReadBackend_KubeMQQueue{
						Conn: &backends.KubeMQQueueConn{},
						Args: &backends.KubeMQQueueReadArgs{},
					},
					RedisPubsub: &protos.ReadCLIConfig_ReadBackend_RedisPubSub{
						Conn: &backends.RedisPubSubConn{},
						Args: &backends.RedisPubSubReadArgs{
							Channel: make([]string, 0),
						},
					},
					RedisStreams: &protos.ReadCLIConfig_ReadBackend_RedisStreams{
						Conn: &backends.RedisStreamsConn{},
						Args: &backends.RedisStreamsReadArgs{
							Stream:               make([]string, 0),
							CreateConsumerConfig: &backends.CreateConsumerConfig{},
						},
					},
					Postgres: &protos.ReadCLIConfig_ReadBackend_Postgres{
						Conn: &backends.PostgresConn{},
						Args: &backends.PostgresReadArgs{},
					},
				},
			},
		},
		Write: &protos.WriteConfig{
			EncodeOptions: &encoding.EncodeOptions{
				Input:    make(map[string]string, 0),
				Metadata: make(map[string]string, 0),
			},
			XCliConfig: &protos.WriteCLIConfig{
				XWriteBackend: &protos.WriteCLIConfig_WriteBackend{
					Kafka: &protos.WriteCLIConfig_WriteBackend_Kafka{
						Conn: &backends.KafkaConn{
							Address: make([]string, 0),
						},
						Args: &backends.KafkaWriteArgs{},
					},
					Activemq: &protos.WriteCLIConfig_WriteBackend_ActiveMQ{
						Conn: &backends.ActiveMQConn{},
						Args: &backends.ActiveMQWriteArgs{},
					},
					Awssqs: &protos.WriteCLIConfig_WriteBackend_AWSSQS{
						Conn: &backends.AWSSQSConn{},
						Args: &backends.AWSSQSWriteArgs{},
					},
					Nats: &protos.WriteCLIConfig_WriteBackend_Nats{
						Conn: &backends.NatsConn{
							TlsCaCert:       make([]byte, 0),
							TlsClientCert:   make([]byte, 0),
							TlsClientKey:    make([]byte, 0),
							UserCredentials: make([]byte, 0),
						},
						Args: &backends.NatsWriteArgs{},
					},
					NatsStreaming: &protos.WriteCLIConfig_WriteBackend_NatsStreaming{
						Conn: &backends.NatsStreamingConn{
							TlsCaCert:       make([]byte, 0),
							TlsClientCert:   make([]byte, 0),
							TlsClientKey:    make([]byte, 0),
							UserCredentials: make([]byte, 0),
						},
						Args: &backends.NatsStreamingWriteArgs{},
					},
					Nsq: &protos.WriteCLIConfig_WriteBackend_NSQ{
						Conn: &backends.NSQConn{
							TlsCaCert:     make([]byte, 0),
							TlsClientCert: make([]byte, 0),
							TlsClientKey:  make([]byte, 0),
						},
						Args: &backends.NSQWriteArgs{},
					},
					Pulsar: &protos.WriteCLIConfig_WriteBackend_Pulsar{
						Conn: &backends.PulsarConn{
							TlsClientCert: make([]byte, 0),
							TlsClientKey:  make([]byte, 0),
						},
						Args: &backends.PulsarWriteArgs{},
					},
					Rabbit: &protos.WriteCLIConfig_WriteBackend_Rabbit{
						Conn: &backends.RabbitConn{},
						Args: &backends.RabbitWriteArgs{},
					},
					RabbitStreams: &protos.WriteCLIConfig_WriteBackend_RabbitStreams{
						Conn: &backends.RabbitStreamsConn{},
						Args: &backends.RabbitStreamsWriteArgs{},
					},
					Mqtt: &protos.WriteCLIConfig_WriteBackend_MQTT{
						Conn: &backends.MQTTConn{
							TlsOptions: &backends.MQTTTLSOptions{},
						},
						Args: &backends.MQTTWriteArgs{},
					},
					AzureServiceBus: &protos.WriteCLIConfig_WriteBackend_AzureServiceBus{
						Conn: &backends.AzureServiceBusConn{},
						Args: &backends.AzureServiceBusWriteArgs{},
					},
					AzureEventHub: &protos.WriteCLIConfig_WriteBackend_AzureEventHub{
						Conn: &backends.AzureEventHubConn{},
						Args: &backends.AzureEventHubWriteArgs{},
					},
					GcpPubsub: &protos.WriteCLIConfig_WriteBackend_GCPPubSub{
						Conn: &backends.GCPPubSubConn{},
						Args: &backends.GCPPubSubWriteArgs{},
					},
					KubemqQueue: &protos.WriteCLIConfig_WriteBackend_KubeMQQueue{
						Conn: &backends.KubeMQQueueConn{},
						Args: &backends.KubeMQQueueWriteArgs{},
					},
					RedisPubsub: &protos.WriteCLIConfig_WriteBackend_RedisPubSub{
						Conn: &backends.RedisPubSubConn{},
						Args: &backends.RedisPubSubWriteArgs{},
					},
					RedisStreams: &protos.WriteCLIConfig_WriteBackend_RedisStreams{
						Conn: &backends.RedisStreamsConn{},
						Args: &backends.RedisStreamsWriteArgs{},
					},
				},
			},
		},
		Relay: &protos.RelayConfig{
			XCliConfig: &protos.CLIRelayConfig{
				XRelayBackend: &protos.CLIRelayConfig_RelayBackend{
					Kafka: &protos.CLIRelayConfig_RelayBackend_Kafka{
						Conn: &backends.KafkaConn{
							Address: make([]string, 0),
						},
						Args: &backends.KafkaRelayArgs{
							Topics: make([]string, 0),
						},
					},
					Awssqs: &protos.CLIRelayConfig_RelayBackend_AWSSQS{
						Conn: &backends.AWSSQSConn{},
						Args: &backends.AWSSQSRelayArgs{},
					},
					Mongo: &protos.CLIRelayConfig_RelayBackend_Mongo{
						Conn: &backends.MongoConn{},
						Args: &backends.MongoReadArgs{},
					},
					Nsq: &protos.CLIRelayConfig_RelayBackend_NSQ{
						Conn: &backends.NSQConn{
							TlsCaCert:     make([]byte, 0),
							TlsClientCert: make([]byte, 0),
							TlsClientKey:  make([]byte, 0),
						},
						Args: &backends.NSQReadArgs{},
					},
					Rabbit: &protos.CLIRelayConfig_RelayBackend_Rabbit{
						Conn: &backends.RabbitConn{},
						Args: &backends.RabbitReadArgs{},
					},
					Mqtt: &protos.CLIRelayConfig_RelayBackend_MQTT{
						Conn: &backends.MQTTConn{
							TlsOptions: &backends.MQTTTLSOptions{},
						},
						Args: &backends.MQTTReadArgs{},
					},
					AzureServiceBus: &protos.CLIRelayConfig_RelayBackend_AzureServiceBus{
						Conn: &backends.AzureServiceBusConn{},
						Args: &backends.AzureServiceBusReadArgs{},
					},
					GcpPubsub: &protos.CLIRelayConfig_RelayBackend_GCPPubSub{
						Conn: &backends.GCPPubSubConn{},
						Args: &backends.GCPPubSubReadArgs{},
					},
					KubemqQueue: &protos.CLIRelayConfig_RelayBackend_KubeMQQueue{
						Conn: &backends.KubeMQQueueConn{},
						Args: &backends.KubeMQQueueReadArgs{},
					},
					RedisPubsub: &protos.CLIRelayConfig_RelayBackend_RedisPubSub{
						Conn: &backends.RedisPubSubConn{},
						Args: &backends.RedisPubSubReadArgs{
							Channel: make([]string, 0),
						},
					},
					RedisStreams: &protos.CLIRelayConfig_RelayBackend_RedisStreams{
						Conn: &backends.RedisStreamsConn{},
						Args: &backends.RedisStreamsReadArgs{
							Stream:               make([]string, 0),
							CreateConsumerConfig: &backends.CreateConsumerConfig{},
						},
					},
					Postgres: &protos.CLIRelayConfig_RelayBackend_Postgres{
						Conn: &backends.PostgresConn{},
						Args: &backends.PostgresReadArgs{},
					},
				},
			},
		},
		Server: &protos.ServerConfig{},
	}
}

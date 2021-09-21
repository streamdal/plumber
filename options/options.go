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
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	VERSION = "UNSET"
)

func New(args []string) (*kong.Context, *opts.CLIOptions, error) {
	cliOpts := newCLIOptions()

	maybeDisplayVersion(os.Args)

	k, err := kong.New(
		cliOpts,
		kong.IgnoreFieldsRegex(".*XXX_"),
		kong.Name("plumber"),
		kong.Description("`curl` for messaging systems. See: https://github.com/batchcorp/plumber"),
		kong.ShortUsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact:             true,
			NoExpandSubcommands: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create new kong instance")
	}

	kongCtx, err := k.Parse(args)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to parse CLI options")
	}

	cliOpts.Global.XAction = kongCtx.Args[0]
	cliOpts.Global.XFullCommand = strings.Join(args, " ")

	if ActionUsesBackend(cliOpts.Global.XAction) {
		if len(args) >= 3 {
			cliOpts.Global.XBackend = args[1]
		}
	}

	logrus.Infof("opts.Global.XAction: %s\n", cliOpts.Global.XAction)
	logrus.Infof("opts.Global.XFullCommand: %s\n", cliOpts.Global.XFullCommand)
	logrus.Infof("opts.Global.XBackend: %s\n", cliOpts.Global.XBackend)

	unsetUnusedOptions(cliOpts)

	return kongCtx, cliOpts, nil
}

func unsetUnusedOptions(opts *opts.CLIOptions) {
	if opts == nil {
		return
	}

	switch opts.Global.XAction {
	case "read":
		unsetUnusedReadOpts(opts)
		opts.Write = nil
		opts.Relay = nil
	case "write":
		unsetUnusedWriteOpts(opts)
		opts.Read = nil
		opts.Relay = nil
	case "relay":
		unsetUnusedRelayOpts(opts)
		opts.Read = nil
		opts.Write = nil
	case "dynamic":
		unsetUnusedDynamicOpts(opts)
	case "batch":
		unsetUnusedBatchOpts(opts)
	}
}

func unsetUnusedDynamicOpts(opts *opts.CLIOptions) {
	// TODO: unset unused backends
}

func unsetUnusedBatchOpts(opts *opts.CLIOptions) {
	// TODO: Unset unused backends
}

func unsetUnusedReadOpts(opts *opts.CLIOptions) {
	// TODO: Unset decode options
	// TODO: Unset sample options
	// TODO: Unset all unused backends
}

func unsetUnusedWriteOpts(opts *opts.CLIOptions) {
	// TODO: Unset encode options
	// TODO: Unset all unused backends
}

func unsetUnusedRelayOpts(opts *opts.CLIOptions) {
	// TODO: Unset all unused backends
}

// ActionUsesBackend checks the action string to determine if a backend will
// need to be utilized. This is used to determine if we need to populate
// XBackend or create a connection config (when in CLI mode).
func ActionUsesBackend(action string) bool {
	switch action {
	case "read":
		return true
	case "relay":
		return true
	case "write":
		return true
	case "dynamic":
		return true
	}

	return false
}

func maybeDisplayVersion(args []string) {
	for _, f := range args {
		if f == "--version" {
			fmt.Println(VERSION)
			os.Exit(0)
		}
	}
}

// We have to do this in order to ensure that kong has valid destinations to
// write opts to.
func newCLIOptions() *opts.CLIOptions {
	return &opts.CLIOptions{
		Global:  &opts.GlobalCLIOptions{},
		Server:  &opts.ServerOptions{},
		Read:    newReadOptions(),
		Write:   newWriteOptions(),
		Relay:   newRelayOptions(),
		Dynamic: newDynamicOptions(),
		Batch:   newBatchOptions(),
	}
}

func newReadOptions() *opts.ReadOptions {
	return &opts.ReadOptions{
		SampleOptions: &opts.ReadSampleOptions{},
		DecodeOptions: &encoding.DecodeOptions{
			ProtobufSettings: &encoding.ProtobufSettings{
				ProtobufDirs: make([]string, 0),
			},
		},
		XCliOptions: &opts.ReadCLIOptions{},
		Kafka: &opts.ReadGroupKafkaOptions{
			XConn: &args.KafkaConn{
				Address: make([]string, 0),
			},
			Args: &args.KafkaReadArgs{
				Topics: make([]string, 0),
			},
		},
		Activemq: &opts.ReadGroupActiveMQOptions{
			XConn: &args.ActiveMQConn{},
			Args:  &args.ActiveMQReadArgs{},
		},
		Awssqs: &opts.ReadGroupAWSSQSOptions{
			XConn: &args.AWSSQSConn{},
			Args:  &args.AWSSQSReadArgs{},
		},
		Mongo: &opts.ReadGroupMongoOptions{
			XConn: &args.MongoConn{},
			Args:  &args.MongoReadArgs{},
		},
		Nats: &opts.ReadGroupNatsOptions{
			XConn: &args.NatsConn{
				TlsCaCert:       make([]byte, 0),
				TlsClientCert:   make([]byte, 0),
				TlsClientKey:    make([]byte, 0),
				UserCredentials: make([]byte, 0),
			},
			Args: &args.NatsReadArgs{},
		},
		NatsStreaming: &opts.ReadGroupNatsStreamingOptions{
			XConn: &args.NatsStreamingConn{
				TlsCaCert:       make([]byte, 0),
				TlsClientCert:   make([]byte, 0),
				TlsClientKey:    make([]byte, 0),
				UserCredentials: make([]byte, 0),
			},
			Args: &args.NatsStreamingReadArgs{},
		},
		Nsq: &opts.ReadGroupNSQOptions{
			XConn: &args.NSQConn{
				TlsCaCert:     make([]byte, 0),
				TlsClientCert: make([]byte, 0),
				TlsClientKey:  make([]byte, 0),
			},
			Args: &args.NSQReadArgs{},
		},
		Pulsar: &opts.ReadGroupPulsarOptions{
			XConn: &args.PulsarConn{
				TlsClientCert: make([]byte, 0),
				TlsClientKey:  make([]byte, 0),
			},
			Args: &args.PulsarReadArgs{},
		},
		Rabbit: &opts.ReadGroupRabbitOptions{
			XConn: &args.RabbitConn{},
			Args:  &args.RabbitReadArgs{},
		},
		RabbitStreams: &opts.ReadGroupRabbitStreamsOptions{
			XConn: &args.RabbitStreamsConn{},
			Args:  &args.RabbitStreamsReadArgs{},
		},
		Mqtt: &opts.ReadGroupMQTTOptions{
			XConn: &args.MQTTConn{
				TlsOptions: &args.MQTTTLSOptions{},
			},
			Args: &args.MQTTReadArgs{},
		},
		AzureServiceBus: &opts.ReadGroupAzureServiceBusOptions{
			XConn: &args.AzureServiceBusConn{},
			Args:  &args.AzureServiceBusReadArgs{},
		},
		AzureEventHub: &opts.ReadGroupAzureEventHubOptions{
			XConn: &args.AzureEventHubConn{},
			Args:  &args.AzureEventHubReadArgs{},
		},
		GcpPubsub: &opts.ReadGroupGCPPubSubOptions{
			XConn: &args.GCPPubSubConn{},
			Args:  &args.GCPPubSubReadArgs{},
		},
		KubemqQueue: &opts.ReadGroupKubeMQQueueOptions{
			XConn: &args.KubeMQQueueConn{},
			Args:  &args.KubeMQQueueReadArgs{},
		},
		RedisPubsub: &opts.ReadGroupRedisPubSubOptions{
			XConn: &args.RedisPubSubConn{},
			Args: &args.RedisPubSubReadArgs{
				Channel: make([]string, 0),
			},
		},
		RedisStreams: &opts.ReadGroupRedisStreamsOptions{
			XConn: &args.RedisStreamsConn{},
			Args: &args.RedisStreamsReadArgs{
				Stream:               make([]string, 0),
				CreateConsumerConfig: &args.CreateConsumerConfig{},
			},
		},
		Postgres: &opts.ReadGroupPostgresOptions{
			XConn: &args.PostgresConn{},
			Args:  &args.PostgresReadArgs{},
		},
	}
}

func newWriteOptions() *opts.WriteOptions {
	return &opts.WriteOptions{
		EncodeOptions: &encoding.EncodeOptions{
			ProtobufSettings: &encoding.ProtobufSettings{},
		},
		Record: &records.WriteRecord{
			InputMetadata: make(map[string]string, 0),
		},
		XCliOptions: &opts.WriteCLIOptions{},
		Kafka: &opts.WriteGroupKafkaOptions{
			XConn: &args.KafkaConn{
				Address: make([]string, 0),
			},
			Args: &args.KafkaWriteArgs{},
		},
		Activemq: &opts.WriteGroupActiveMQOptions{
			XConn: &args.ActiveMQConn{},
			Args:  &args.ActiveMQWriteArgs{},
		},
		Awssqs: &opts.WriteGroupAWSSQSOptions{
			XConn: &args.AWSSQSConn{},
			Args:  &args.AWSSQSWriteArgs{},
		},
		Nats: &opts.WriteGroupNatsOptions{
			XConn: &args.NatsConn{
				TlsCaCert:       make([]byte, 0),
				TlsClientCert:   make([]byte, 0),
				TlsClientKey:    make([]byte, 0),
				UserCredentials: make([]byte, 0),
			},
			Args: &args.NatsWriteArgs{},
		},
		NatsStreaming: &opts.WriteGroupNatsStreamingOptions{
			XConn: &args.NatsStreamingConn{
				TlsCaCert:       make([]byte, 0),
				TlsClientCert:   make([]byte, 0),
				TlsClientKey:    make([]byte, 0),
				UserCredentials: make([]byte, 0),
			},
			Args: &args.NatsStreamingWriteArgs{},
		},
		Nsq: &opts.WriteGroupNSQOptions{
			XConn: &args.NSQConn{
				TlsCaCert:     make([]byte, 0),
				TlsClientCert: make([]byte, 0),
				TlsClientKey:  make([]byte, 0),
			},
			Args: &args.NSQWriteArgs{},
		},
		Pulsar: &opts.WriteGroupPulsarOptions{
			XConn: &args.PulsarConn{
				TlsClientCert: make([]byte, 0),
				TlsClientKey:  make([]byte, 0),
			},
			Args: &args.PulsarWriteArgs{},
		},
		Rabbit: &opts.WriteGroupRabbitOptions{
			XConn: &args.RabbitConn{},
			Args:  &args.RabbitWriteArgs{},
		},
		RabbitStreams: &opts.WriteGroupRabbitStreamsOptions{
			XConn: &args.RabbitStreamsConn{},
			Args:  &args.RabbitStreamsWriteArgs{},
		},
		Mqtt: &opts.WriteGroupMQTTOptions{
			XConn: &args.MQTTConn{
				TlsOptions: &args.MQTTTLSOptions{},
			},
			Args: &args.MQTTWriteArgs{},
		},
		AzureServiceBus: &opts.WriteGroupAzureServiceBusOptions{
			XConn: &args.AzureServiceBusConn{},
			Args:  &args.AzureServiceBusWriteArgs{},
		},
		AzureEventHub: &opts.WriteGroupAzureEventHubOptions{
			XConn: &args.AzureEventHubConn{},
			Args:  &args.AzureEventHubWriteArgs{},
		},
		GcpPubsub: &opts.WriteGroupGCPPubSubOptions{
			XConn: &args.GCPPubSubConn{},
			Args:  &args.GCPPubSubWriteArgs{},
		},
		KubemqQueue: &opts.WriteGroupKubeMQQueueOptions{
			XConn: &args.KubeMQQueueConn{},
			Args:  &args.KubeMQQueueWriteArgs{},
		},
		RedisPubsub: &opts.WriteGroupRedisPubSubOptions{
			XConn: &args.RedisPubSubConn{},
			Args:  &args.RedisPubSubWriteArgs{},
		},
		RedisStreams: &opts.WriteGroupRedisStreamsOptions{
			XConn: &args.RedisStreamsConn{},
			Args:  &args.RedisStreamsWriteArgs{},
		},
	}

}

func newRelayOptions() *opts.RelayOptions {
	return &opts.RelayOptions{
		XCliOptions: &opts.RelayCLIOptions{},
		Kafka: &opts.RelayGroupKafkaOptions{
			XConn: &args.KafkaConn{
				Address: make([]string, 0),
			},
			Args: &args.KafkaRelayArgs{
				Topics: make([]string, 0),
			},
		},
		Awssqs: &opts.RelayGroupAWSSQSOptions{
			XConn: &args.AWSSQSConn{},
			Args:  &args.AWSSQSRelayArgs{},
		},
		Mongo: &opts.RelayGroupMongoOptions{
			XConn: &args.MongoConn{},
			Args:  &args.MongoReadArgs{},
		},
		Nsq: &opts.RelayGroupNSQOptions{
			XConn: &args.NSQConn{
				TlsCaCert:     make([]byte, 0),
				TlsClientCert: make([]byte, 0),
				TlsClientKey:  make([]byte, 0),
			},
			Args: &args.NSQReadArgs{},
		},
		Rabbit: &opts.RelayGroupRabbitOptions{
			XConn: &args.RabbitConn{},
			Args:  &args.RabbitReadArgs{},
		},
		Mqtt: &opts.RelayGroupMQTTOptions{
			XConn: &args.MQTTConn{
				TlsOptions: &args.MQTTTLSOptions{},
			},
			Args: &args.MQTTReadArgs{},
		},
		AzureServiceBus: &opts.RelayGroupAzureServiceBusOptions{
			XConn: &args.AzureServiceBusConn{},
			Args:  &args.AzureServiceBusReadArgs{},
		},
		GcpPubsub: &opts.RelayGroupGCPPubSubOptions{
			XConn: &args.GCPPubSubConn{},
			Args:  &args.GCPPubSubReadArgs{},
		},
		KubemqQueue: &opts.RelayGroupKubeMQQueueOptions{
			XConn: &args.KubeMQQueueConn{},
			Args:  &args.KubeMQQueueReadArgs{},
		},
		RedisPubsub: &opts.RelayGroupRedisPubSubOptions{
			XConn: &args.RedisPubSubConn{},
			Args: &args.RedisPubSubReadArgs{
				Channel: make([]string, 0),
			},
		},
		RedisStreams: &opts.RelayGroupRedisStreamsOptions{
			XConn: &args.RedisStreamsConn{},
			Args: &args.RedisStreamsReadArgs{
				Stream:               make([]string, 0),
				CreateConsumerConfig: &args.CreateConsumerConfig{},
			},
		},
		Postgres: &opts.RelayGroupPostgresOptions{
			XConn: &args.PostgresConn{},
			Args:  &args.PostgresReadArgs{},
		},
	}
}

func newDynamicOptions() *opts.DynamicOptions {
	return &opts.DynamicOptions{
		Kafka: &opts.DynamicGroupKafkaOptions{
			XConn: &args.KafkaConn{
				Address: make([]string, 0),
			},
			Args: &args.KafkaWriteArgs{},
		},
		Activemq: &opts.DynamicGroupActiveMQOptions{
			XConn: &args.ActiveMQConn{},
			Args:  &args.ActiveMQWriteArgs{},
		},
		Awssqs: &opts.DynamicGroupAWSSQSOptions{
			XConn: &args.AWSSQSConn{},
			Args:  &args.AWSSQSWriteArgs{},
		},
		Nats: &opts.DynamicGroupNatsOptions{
			XConn: &args.NatsConn{
				TlsCaCert:       make([]byte, 0),
				TlsClientCert:   make([]byte, 0),
				TlsClientKey:    make([]byte, 0),
				UserCredentials: make([]byte, 0),
			},
			Args: &args.NatsWriteArgs{},
		},
		NatsStreaming: &opts.DynamicGroupNatsStreamingOptions{
			XConn: &args.NatsStreamingConn{
				TlsCaCert:       make([]byte, 0),
				TlsClientCert:   make([]byte, 0),
				TlsClientKey:    make([]byte, 0),
				UserCredentials: make([]byte, 0),
			},
			Args: &args.NatsStreamingWriteArgs{},
		},
		Nsq: &opts.DynamicGroupNSQOptions{
			XConn: &args.NSQConn{
				TlsCaCert:     make([]byte, 0),
				TlsClientCert: make([]byte, 0),
				TlsClientKey:  make([]byte, 0),
			},
			Args: &args.NSQWriteArgs{},
		},
		Rabbit: &opts.DynamicGroupRabbitOptions{
			XConn: &args.RabbitConn{},
			Args:  &args.RabbitWriteArgs{},
		},
		Mqtt: &opts.DynamicGroupMQTTOptions{
			XConn: &args.MQTTConn{
				TlsOptions: &args.MQTTTLSOptions{},
			},
			Args: &args.MQTTWriteArgs{},
		},
		AzureServiceBus: &opts.DynamicGroupAzureServiceBusOptions{
			XConn: &args.AzureServiceBusConn{},
			Args:  &args.AzureServiceBusWriteArgs{},
		},
		AzureEventHub: &opts.DynamicGroupAzureEventHubOptions{
			XConn: &args.AzureEventHubConn{},
			Args:  &args.AzureEventHubWriteArgs{},
		},
		GcpPubsub: &opts.DynamicGroupGCPPubSubOptions{
			XConn: &args.GCPPubSubConn{},
			Args:  &args.GCPPubSubWriteArgs{},
		},
		KubemqQueue: &opts.DynamicGroupKubeMQQueueOptions{
			XConn: &args.KubeMQQueueConn{},
			Args:  &args.KubeMQQueueWriteArgs{},
		},
		RedisPubsub: &opts.DynamicGroupRedisPubSubOptions{
			XConn: &args.RedisPubSubConn{},
			Args:  &args.RedisPubSubWriteArgs{},
		},
		RedisStreams: &opts.DynamicGroupRedisStreamsOptions{
			XConn: &args.RedisStreamsConn{},
			Args:  &args.RedisStreamsWriteArgs{},
		},
	}
}

func newBatchOptions() *opts.BatchOptions {
	return &opts.BatchOptions{
		Login:  &opts.BatchLoginOptions{},
		Logout: &opts.BatchLogoutOptions{},
		List:   &opts.BatchListOptions{},
		Create: &opts.BatchCreateOptions{
			Collection: &opts.BatchCreateCollectionOptions{},
			Replay:     &opts.BatchCreateReplayOptions{},
			Destination: &opts.BatchCreateDestinationOptions{
				Kafka: &opts.WriteGroupKafkaOptions{
					XConn: &args.KafkaConn{},
					Args: &args.KafkaWriteArgs{
						Headers: make(map[string]string, 0),
						Topics:  make([]string, 0),
					},
				},
				Rabbit: &opts.WriteGroupRabbitOptions{
					XConn: &args.RabbitConn{},
					Args:  &args.RabbitWriteArgs{},
				},
				KubemqQueue: &opts.WriteGroupKubeMQQueueOptions{
					XConn: &args.KubeMQQueueConn{},
					Args:  &args.KubeMQQueueWriteArgs{},
				},
				Awssqs: &opts.WriteGroupAWSSQSOptions{
					XConn: &args.AWSSQSConn{},
					Args: &args.AWSSQSWriteArgs{
						Attributes: make(map[string]string, 0),
					},
				},
				Http: &opts.HTTPDestination{
					Headers: make(map[string]string, 0),
				},
			},
		},
		Search: &opts.BatchSearchOptions{},
		Archive: &opts.BatchArchiveOptions{
			Replay: &opts.BatchArchiveReplayOptions{},
		},
	}
}

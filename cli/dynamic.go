package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type DynamicOptions struct {
}

func HandleDynamicFlags(dynamicCmd *kingpin.CmdClause, opts *Options) {
	kafkaCmd := dynamicCmd.Command("kafka", "Kafka as Replay Destination")
	addSharedKafkaFlags(kafkaCmd, opts)
	addWriteKafkaFlags(kafkaCmd, opts)

	rabbitCmd := dynamicCmd.Command("rabbit", "RabbitMQ as Replay Destination")
	addSharedRabbitFlags(rabbitCmd, opts)
	addWriteKafkaFlags(rabbitCmd, opts)

	mqttCmd := dynamicCmd.Command("mqtt", "MQTT as Replay Destination")
	addSharedMQTTFlags(mqttCmd, opts)
	addWriteMQTTFlags(mqttCmd, opts)

	rPubSubCmd := dynamicCmd.Command("redis-pubsub", "Redis PubSub as Replay Destination")
	addSharedRedisPubSubFlags(rPubSubCmd, opts)

	rStreams := dynamicCmd.Command("redis-streams", "Redis Streams as Replay Destination")
	addSharedRedisStreamsFlags(rStreams, opts)
	addWriteRedisStreamsFlags(rStreams, opts)

	natsCmd := dynamicCmd.Command("nats", "NATS as Replay Destination")
	addSharedNatsFlags(natsCmd, opts)

	natsStreamingCmd := dynamicCmd.Command("nats-streaming", "NATS Streaming as Replay Destination")
	addSharedNatsFlags(natsStreamingCmd, opts)
}

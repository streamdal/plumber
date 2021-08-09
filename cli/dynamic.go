package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type DynamicOptions struct {
}

func HandleDynamicFlags(dynamicCmd *kingpin.CmdClause, opts *Options) {
	amqCmd := dynamicCmd.Command("activemq", "ActiveMQ as Replay Destination")
	addSharedActiveMqFlags(amqCmd, opts)

	snsCmd := dynamicCmd.Command("aws-sns", "AWS SNS as Replay Destination")
	addWriteAWSSNSFlags(snsCmd, opts)

	sqsCmd := dynamicCmd.Command("aws-sqs", "AWS SQS as Replay Destination")
	addSharedAWSSQSFlags(sqsCmd, opts)
	addWriteAWSSQSFlags(sqsCmd, opts)

	azureCmd := dynamicCmd.Command("azure", "Azure Service Bus as Replay Destination")
	addSharedAzureFlags(azureCmd, opts)

	azureEventHubCmd := dynamicCmd.Command("azure-eventhub", "Azure Event Hub as Replay Destination")
	addSharedAzureEventhubFlags(azureEventHubCmd, opts)
	addWriteAzureEventhubFlags(azureEventHubCmd, opts)

	gcpCmd := dynamicCmd.Command("gcp-pubsub", "GCP PubSub as Replay Destination")
	addSharedGCPPubSubFlags(gcpCmd, opts)
	addWriteGCPPubSubFlags(gcpCmd, opts)

	kafkaCmd := dynamicCmd.Command("kafka", "Kafka as Replay Destination")
	addSharedKafkaFlags(kafkaCmd, opts)
	addWriteKafkaFlags(kafkaCmd, opts)

	mqttCmd := dynamicCmd.Command("mqtt", "MQTT as Replay Destination")
	addSharedMQTTFlags(mqttCmd, opts)
	addWriteMQTTFlags(mqttCmd, opts)

	natsCmd := dynamicCmd.Command("nats", "NATS as Replay Destination")
	addSharedNatsFlags(natsCmd, opts)

	natsStreamingCmd := dynamicCmd.Command("nats-streaming", "NATS Streaming as Replay Destination")
	addSharedNatsStreamingFlags(natsStreamingCmd, opts)

	rabbitCmd := dynamicCmd.Command("rabbit", "RabbitMQ as Replay Destination")
	addSharedRabbitFlags(rabbitCmd, opts)
	addWriteRabbitFlags(rabbitCmd, opts)

	rPubSubCmd := dynamicCmd.Command("redis-pubsub", "Redis PubSub as Replay Destination")
	addSharedRedisPubSubFlags(rPubSubCmd, opts)

	rStreams := dynamicCmd.Command("redis-streams", "Redis Streams as Replay Destination")
	addSharedRedisStreamsFlags(rStreams, opts)
	addWriteRedisStreamsFlags(rStreams, opts)

	kubemqQueueCmd := dynamicCmd.Command("kubemq-queue", "KubeMQ Queue as a Replay Destination")
	addSharedKubeMQQueueFlags(kubemqQueueCmd, opts)

}

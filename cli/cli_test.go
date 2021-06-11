package cli

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestHandleRabbitFlags_read(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"read", "rabbit",
		"--address", "amqp://testing.tld:6379",
		"--exchange", "testex",
		"--routing-key", "testqueue",
		"--queue", "testqueue",
		"--queue-durable",        // default is false
		"--no-queue-auto-delete", // default is true
		"--no-queue-exclusive",   // default is true
		"--no-queue-declare",     // default is true
		"--no-auto-ack",          // default is true
		"--consumer-tag", "plumber_123",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("read rabbit"))
	g.Expect(opts.Rabbit.Exchange).To(Equal("testex"))
	g.Expect(opts.Rabbit.Address).To(Equal("amqp://testing.tld:6379"))
	g.Expect(opts.Rabbit.RoutingKey).To(Equal("testqueue"))
	g.Expect(opts.Rabbit.ReadQueueDurable).To(BeTrue())
	g.Expect(opts.Rabbit.ReadQueueAutoDelete).To(BeFalse())
	g.Expect(opts.Rabbit.ReadQueueExclusive).To(BeFalse())
	g.Expect(opts.Rabbit.ReadQueueDeclare).To(BeFalse())
	g.Expect(opts.Rabbit.ReadAutoAck).To(BeFalse())
	g.Expect(opts.Rabbit.ReadConsumerTag).To(Equal("plumber_123"))
	g.Expect(opts.ReadProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.ReadProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleRabbitFlags_relay(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"relay", "rabbit",
		"--address", "amqp://testing.tld:6379",
		"--type", "rabbit",
		"--token", "8EDB98ED-0D85-4CFD-BE24-8B1E00A9F7C3",
		"--grpc-disable-tls",
		"--grpc-timeout", "4s",
		"--grpc-address", "localhost:9000",
		"--num-workers", "5",
		"--exchange", "testex",
		"--routing-key", "testqueue",
		"--queue", "testqueue",
		"--queue-durable",        // default is false
		"--no-queue-auto-delete", // default is true
		"--no-queue-exclusive",   // default is true
		"--no-queue-declare",     // default is true
		"--no-auto-ack",          // default is true
		"--consumer-tag", "plumber_123",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("relay rabbit"))
	g.Expect(opts.RelayType).To(Equal("rabbit"))
	g.Expect(opts.RelayGRPCDisableTLS).To(BeTrue())
	g.Expect(opts.RelayGRPCTimeout).To(Equal(time.Second * 4))
	g.Expect(opts.RelayToken).To(Equal("8EDB98ED-0D85-4CFD-BE24-8B1E00A9F7C3"))
	g.Expect(opts.RelayGRPCAddress).To(Equal("localhost:9000"))
	g.Expect(opts.RelayNumWorkers).To(Equal(5))
	g.Expect(opts.Rabbit.Exchange).To(Equal("testex"))
	g.Expect(opts.Rabbit.Address).To(Equal("amqp://testing.tld:6379"))
	g.Expect(opts.Rabbit.RoutingKey).To(Equal("testqueue"))
	g.Expect(opts.Rabbit.ReadQueueDurable).To(BeTrue())
	g.Expect(opts.Rabbit.ReadQueueAutoDelete).To(BeFalse())
	g.Expect(opts.Rabbit.ReadQueueExclusive).To(BeFalse())
	g.Expect(opts.Rabbit.ReadQueueDeclare).To(BeFalse())
	g.Expect(opts.Rabbit.ReadAutoAck).To(BeFalse())
	g.Expect(opts.Rabbit.ReadConsumerTag).To(Equal("plumber_123"))
	g.Expect(opts.ReadProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.ReadProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleRabbitFlags_write(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"write", "rabbit",
		"--address", "amqp://testing.tld:6379",
		"--exchange", "testex",
		"--routing-key", "testqueue",
		"--app-id", "plumber_123",
		"--input-data", "welovemessaging",
		"--input-file", "cli.go",
		"--input-type", "jsonpb",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("write rabbit"))
	g.Expect(opts.Rabbit.Exchange).To(Equal("testex"))
	g.Expect(opts.Rabbit.Address).To(Equal("amqp://testing.tld:6379"))
	g.Expect(opts.Rabbit.RoutingKey).To(Equal("testqueue"))
	g.Expect(opts.Rabbit.WriteAppID).To(Equal("plumber_123"))
	g.Expect(opts.WriteInputData).To(Equal("welovemessaging"))
	g.Expect(opts.WriteInputFile).To(Equal("cli.go"))
	g.Expect(opts.WriteInputType).To(Equal("jsonpb"))
	g.Expect(opts.WriteProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.WriteProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleMQTTFlags_read(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"read", "mqtt",
		"--address", "tcp://testing.tld:1883",
		"--topic", "plumber_test",
		"--timeout", "3s",
		"--read-timeout", "10s",
		"--qos", "2",
		"--tls-ca-file", "cli.go",
		"--tls-client-cert-file", "cli.go",
		"--tls-client-key-file", "cli.go",
		"--insecure-tls", // default is false
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("read mqtt"))
	g.Expect(opts.MQTT.Address).To(Equal("tcp://testing.tld:1883"))
	g.Expect(opts.MQTT.Topic).To(Equal("plumber_test"))
	g.Expect(opts.MQTT.Timeout).To(Equal(time.Second * 3))
	g.Expect(opts.MQTT.ReadTimeout).To(Equal(time.Second * 10))
	g.Expect(opts.MQTT.QoSLevel).To(Equal(2))
	g.Expect(opts.MQTT.TLSCAFile).To(Equal("cli.go"))
	g.Expect(opts.MQTT.TLSClientKeyFile).To(Equal("cli.go"))
	g.Expect(opts.MQTT.TLSClientCertFile).To(Equal("cli.go"))
	g.Expect(opts.ReadProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.ReadProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleMQTTFlags_write(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"write", "mqtt",
		"--address", "tcp://testing.tld:1883",
		"--topic", "plumber_test",
		"--timeout", "3s",
		"--write-timeout", "10s",
		"--qos", "2",
		"--tls-ca-file", "cli.go",
		"--tls-client-cert-file", "cli.go",
		"--tls-client-key-file", "cli.go",
		"--insecure-tls", // default is false
		"--input-data", "welovemessaging",
		"--input-file", "cli.go",
		"--input-type", "jsonpb",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("write mqtt"))
	g.Expect(opts.MQTT.Address).To(Equal("tcp://testing.tld:1883"))
	g.Expect(opts.MQTT.Topic).To(Equal("plumber_test"))
	g.Expect(opts.MQTT.Timeout).To(Equal(time.Second * 3))
	g.Expect(opts.MQTT.WriteTimeout).To(Equal(time.Second * 10))
	g.Expect(opts.MQTT.QoSLevel).To(Equal(2))
	g.Expect(opts.MQTT.TLSCAFile).To(Equal("cli.go"))
	g.Expect(opts.MQTT.TLSClientKeyFile).To(Equal("cli.go"))
	g.Expect(opts.MQTT.TLSClientCertFile).To(Equal("cli.go"))
	g.Expect(opts.WriteInputData).To(Equal("welovemessaging"))
	g.Expect(opts.WriteInputFile).To(Equal("cli.go"))
	g.Expect(opts.WriteInputType).To(Equal("jsonpb"))
	g.Expect(opts.WriteProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.WriteProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleKafkaFlags_read(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"read", "kafka",
		"--address", "testing.tld:9092",
		"--topic", "plumber_test",
		"--group-id", "plumber_test_group",
		"--timeout", "3s",
		"--insecure-tls", // default is false
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("read kafka"))
	g.Expect(opts.Kafka.Brokers[0]).To(Equal("testing.tld:9092"))
	g.Expect(opts.Kafka.Topics).To(Equal([]string{"plumber_test"}))
	g.Expect(opts.Kafka.GroupID).To(Equal("plumber_test_group"))
	g.Expect(opts.Kafka.Timeout).To(Equal(time.Second * 3))
	g.Expect(opts.ReadProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.ReadProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleKafkaFlags_write(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"write", "kafka",
		"--address", "testing.tld:9092",
		"--topic", "plumber_test",
		"--topic", "plumber_test_2",
		"--key", "plumber_test_key",
		"--timeout", "3s",
		"--insecure-tls", // default is false
		"--input-data", "welovemessaging",
		"--input-file", "cli.go",
		"--input-type", "jsonpb",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("write kafka"))
	g.Expect(opts.Kafka.Brokers[0]).To(Equal("testing.tld:9092"))
	g.Expect(opts.Kafka.Topics).To(Equal([]string{"plumber_test", "plumber_test_2"}))
	g.Expect(opts.Kafka.WriteKey).To(Equal("plumber_test_key"))
	g.Expect(opts.Kafka.Timeout).To(Equal(time.Second * 3))
	g.Expect(opts.WriteInputData).To(Equal("welovemessaging"))
	g.Expect(opts.WriteInputFile).To(Equal("cli.go"))
	g.Expect(opts.WriteInputType).To(Equal("jsonpb"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleAWSSQSFlags_read(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"read", "aws-sqs",
		"--queue-name", "plumber_test",
		"--remote-account-id", "1234",
		"--auto-delete", // default is false
		"--max-num-messages", "1",
		"--wait-time-seconds", "3",
		"--receive-request-attempt-id", "plumber_receiver",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("read aws-sqs"))
	g.Expect(opts.AWSSQS.QueueName).To(Equal("plumber_test"))
	g.Expect(opts.AWSSQS.RemoteAccountID).To(Equal("1234"))
	g.Expect(opts.AWSSQS.ReadWaitTimeSeconds).To(Equal(int64(3)))
	g.Expect(opts.AWSSQS.ReadReceiveRequestAttemptId).To(Equal("plumber_receiver"))
	g.Expect(opts.ReadProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.ReadProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleAWSSQSFlags_write(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"write", "aws-sqs",
		"--queue-name", "plumber_test",
		"--remote-account-id", "1234",
		"--delay-seconds", "6",
		"--attributes", "tag=value",
		"--input-data", "welovemessaging",
		"--input-file", "cli.go",
		"--input-type", "jsonpb",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("write aws-sqs"))
	g.Expect(opts.AWSSQS.QueueName).To(Equal("plumber_test"))
	g.Expect(opts.AWSSQS.RemoteAccountID).To(Equal("1234"))
	g.Expect(opts.AWSSQS.WriteDelaySeconds).To(Equal(int64(6)))
	g.Expect(opts.AWSSQS.WriteAttributes).To(Equal(map[string]string{"tag": "value"}))
	g.Expect(opts.WriteInputData).To(Equal("welovemessaging"))
	g.Expect(opts.WriteInputFile).To(Equal("cli.go"))
	g.Expect(opts.WriteInputType).To(Equal("jsonpb"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

//--project-id=PROJECT-ID    Project Id
//--sub-id=SUB-ID            Subscription Id
//--ack                      Whether to acknowledge message receive
func TestHandleGCPPubSubFlags_read(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"read", "gcp-pubsub",
		"--sub-id", "plumber_sub",
		"--project-id", "plumber_project",
		"--no-ack", // default is true
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("read gcp-pubsub"))
	g.Expect(opts.GCPPubSub.ReadSubscriptionId).To(Equal("plumber_sub"))
	g.Expect(opts.GCPPubSub.ProjectId).To(Equal("plumber_project"))
	g.Expect(opts.GCPPubSub.ReadAck).To(BeFalse())
	g.Expect(opts.ReadProtobufDirs).To(Equal([]string{"../test-assets/protos"}))
	g.Expect(opts.ReadProtobufRootMessage).To(Equal("Message"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

func TestHandleGCPPubSubFlags_write(t *testing.T) {

	g := NewGomegaWithT(t)

	args := []string{
		"write", "gcp-pubsub",
		"--project-id", "plumber_project",
		"--topic-id", "plumber_topic",
		"--input-data", "welovemessaging",
		"--input-file", "cli.go",
		"--input-type", "jsonpb",
		"--protobuf-dir", "../test-assets/protos",
		"--protobuf-root-message", "Message",
		"--avro-schema", "../test-assets/avro/test.avsc",
	}

	cmd, opts, err := Handle(args)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("write gcp-pubsub"))
	g.Expect(opts.GCPPubSub.WriteTopicId).To(Equal("plumber_topic"))
	g.Expect(opts.GCPPubSub.ProjectId).To(Equal("plumber_project"))
	g.Expect(opts.WriteInputData).To(Equal("welovemessaging"))
	g.Expect(opts.WriteInputFile).To(Equal("cli.go"))
	g.Expect(opts.WriteInputType).To(Equal("jsonpb"))
	g.Expect(opts.AvroSchemaFile).To(Equal("../test-assets/avro/test.avsc"))
}

package options

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestHandleRabbitEnvars_relay(t *testing.T) {

	g := NewGomegaWithT(t)

	envars := map[string]string{
		"PLUMBER_DEBUG":                          "true",
		"PLUMBER_RELAY_TYPE":                     "rabbit",
		"PLUMBER_RELAY_TOKEN":                    "8EDB98ED-0D85-4CFD-BE24-8B1E00A9F7C3",
		"PLUMBER_RELAY_GRPC_ADDRESS":             "localhost:9000",
		"PLUMBER_RELAY_GRPC_DISABLE_TLS":         "true",
		"PLUMBER_RELAY_GRPC_TIMEOUT":             "4s",
		"PLUMBER_RELAY_NUM_WORKERS":              "10",
		"PLUMBER_RELAY_RABBIT_ADDRESS":           "amqp://testing.tld:6379",
		"PLUMBER_RELAY_RABBIT_EXCHANGE":          "testex",
		"PLUMBER_RELAY_RABBIT_ROUTING_KEY":       "testqueue",
		"PLUMBER_RELAY_RABBIT_QUEUE":             "testqueue",
		"PLUMBER_RELAY_RABBIT_QUEUE_DURABLE":     "true",
		"PLUMBER_RELAY_RABBIT_QUEUE_AUTO_DELETE": "false",
		"PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE":   "false",
		"PLUMBER_RELAY_RABBIT_AUTOACK":           "false",
		"PLUMBER_RELAY_RABBIT_QUEUE_DECLARE":     "false",
		"PLUMBER_RELAY_CONSUMER_TAG":             "plumber_123",
	}

	for k, v := range envars {
		os.Setenv(k, v)
	}

	defer func() {
		// Unset all so we don't interfere with other tests
		for k, _ := range envars {
			os.Unsetenv(k)
		}
	}()

	cmd, opts, err := Handle([]string{"relay"})

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("relay"))
	g.Expect(opts.RelayType).To(Equal("rabbit"))
	g.Expect(opts.RelayGRPCDisableTLS).To(BeTrue())
	g.Expect(opts.RelayGRPCTimeout).To(Equal(time.Second * 4))
	g.Expect(opts.RelayToken).To(Equal("8EDB98ED-0D85-4CFD-BE24-8B1E00A9F7C3"))
	g.Expect(opts.RelayGRPCAddress).To(Equal("localhost:9000"))
	g.Expect(opts.RelayNumWorkers).To(Equal(10))
	g.Expect(opts.Rabbit.Exchange).To(Equal("testex"))
	g.Expect(opts.Rabbit.Address).To(Equal("amqp://testing.tld:6379"))
	g.Expect(opts.Rabbit.RoutingKey).To(Equal("testqueue"))
	g.Expect(opts.Rabbit.ReadQueueDurable).To(BeTrue())
	g.Expect(opts.Rabbit.ReadQueueAutoDelete).To(BeFalse())
	g.Expect(opts.Rabbit.ReadQueueExclusive).To(BeFalse())
	g.Expect(opts.Rabbit.ReadQueueDeclare).To(BeFalse())
	g.Expect(opts.Rabbit.ReadAutoAck).To(BeFalse())
	g.Expect(opts.Rabbit.ReadConsumerTag).To(Equal("plumber_123"))

}

func TestHandleAWSSQSEnvars_relay(t *testing.T) {

	g := NewGomegaWithT(t)

	envars := map[string]string{
		"PLUMBER_DEBUG":                                "true",
		"PLUMBER_RELAY_TYPE":                           "aws-sqs",
		"PLUMBER_RELAY_TOKEN":                          "8EDB98ED-0D85-4CFD-BE24-8B1E00A9F7C3",
		"PLUMBER_RELAY_GRPC_ADDRESS":                   "localhost:9000",
		"PLUMBER_RELAY_GRPC_DISABLE_TLS":               "true",
		"PLUMBER_RELAY_GRPC_TIMEOUT":                   "4s",
		"PLUMBER_RELAY_NUM_WORKERS":                    "10",
		"PLUMBER_RELAY_SQS_QUEUE_NAME":                 "plumber_test",
		"PLUMBER_RELAY_SQS_REMOTE_ACCOUNT_ID":          "1234",
		"PLUMBER_RELAY_SQS_MAX_NUM_MESSAGES":           "2",
		"PLUMBER_RELAY_SQS_RECEIVE_REQUEST_ATTEMPT_ID": "plumber_receiver",
		"PLUMBER_RELAY_SQS_AUTO_DELETE":                "true",
		"PLUMBER_RELAY_SQS_WAIT_TIME_SECONDS":          "6",
		"PLUMBER_RELAY_CONSUMER_TAG":                   "plumber_123",
	}

	for k, v := range envars {
		os.Setenv(k, v)
	}

	defer func() {
		// Unset all so we don't interfere with other tests
		for k, _ := range envars {
			os.Unsetenv(k)
		}
	}()

	cmd, opts, err := Handle([]string{"relay"})

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(cmd).To(Equal("relay"))
	g.Expect(opts.Debug).To(BeTrue())
	g.Expect(opts.RelayType).To(Equal("aws-sqs"))
	g.Expect(opts.RelayToken).To(Equal("8EDB98ED-0D85-4CFD-BE24-8B1E00A9F7C3"))
	g.Expect(opts.RelayGRPCAddress).To(Equal("localhost:9000"))
	g.Expect(opts.RelayGRPCDisableTLS).To(BeTrue())
	g.Expect(opts.RelayGRPCTimeout).To(Equal(time.Second * 4))
	g.Expect(opts.RelayNumWorkers).To(Equal(10))
	g.Expect(opts.AWSSQS.QueueName).To(Equal("plumber_test"))
	g.Expect(opts.AWSSQS.RemoteAccountID).To(Equal("1234"))
	g.Expect(opts.AWSSQS.RelayMaxNumMessages).To(Equal(int64(2)))
	g.Expect(opts.AWSSQS.RelayAutoDelete).To(BeTrue())
	g.Expect(opts.AWSSQS.RemoteAccountID).To(Equal("1234"))
	g.Expect(opts.AWSSQS.RelayWaitTimeSeconds).To(Equal(int64(6)))
	g.Expect(opts.AWSSQS.RelayReceiveRequestAttemptId).To(Equal("plumber_receiver"))
}

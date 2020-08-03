// This package will perform _functional_ testing of the plumber CLI tool.
//
// It is going to perform a single, OS-specific compile via `make` and then
// re-execute the built binary over and over.
//
// NOTE: You should probably have local instances of rabbit, kafka, etc. running
// or  else the test suite will fail.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"runtime"
	"time"

	"cloud.google.com/go/pubsub"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"
)

var (
	kafkaReader     *skafka.Reader
	kafkaWriter     *skafka.Writer
	rabbitChannel   *amqp.Channel
	gcpPubSubClient *pubsub.Client
)

var _ = Describe("Functional", func() {
	var (
		kafkaAddress         = "localhost:9092"
		kafkaTopic           = "test"
		gcpPubSubProjectId   = "projectId"
		rabbitAddress        = "amqp://localhost:5672"
		binary               = "./build/plumber-" + runtime.GOOS
		sampleOutboundJSONPB = "./test-assets/messages/sample-outbound.json"
		protoSchemasDir      = "./test-assets/protos"
	)

	BeforeSuite(func() {
		if err := setupConnections(
			kafkaAddress,
			kafkaTopic,
			gcpPubSubProjectId,
			rabbitAddress,
		); err != nil {
			Fail(fmt.Sprintf("unable to setup connection(s): %s", err))
		}

		// Build the application
		var cmd *exec.Cmd
		var err error

		switch runtime.GOOS {
		case "linux":
			cmd = exec.Command("make", "build/linux")
			_, err = cmd.CombinedOutput()
		case "darwin":
			cmd = exec.Command("make", "build/darwin")
			_, err = cmd.CombinedOutput()
		default:
			Fail(fmt.Sprintf("unsupported GOOS '%s'", runtime.GOOS))
		}

		if err != nil {
			Fail(fmt.Sprintf("unable to build binary: %s", err))
		}

		Expect(binary).To(BeAnExistingFile())
	})

	Describe("Kafka", func() {
		Describe("write", func() {
			Context("plain input, plain output", func() {
				It("should work", func() {
					randString := fmt.Sprintf("kafka-random-%d", rand.Int())

					cmd := exec.Command(binary, "write", "message", "kafka", "--address", kafkaAddress,
						"--topic", kafkaTopic, "--input-data", randString)

					_, err := cmd.CombinedOutput()

					Expect(err).ToNot(HaveOccurred())

					// Read message from kafka topic, verify it's set to randString
					value, err := getMessageFromKafka(kafkaAddress, kafkaTopic)

					Expect(err).ToNot(HaveOccurred())
					Expect(string(value)).To(Equal(randString))
				})
			})

			FContext("jsonpb input, protobuf output", func() {
				It("should work", func() {
					cmd := exec.Command(binary, "write", "message", "kafka", "--address", kafkaAddress,
						"--topic", kafkaTopic, "--input-type", "jsonpb", "--output-type", "protobuf",
						"--input-file", sampleOutboundJSONPB, "--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound")

					output, err := cmd.CombinedOutput()

					fmt.Println(string(output))

					Expect(err).ToNot(HaveOccurred())

					// Read message from kafka topic; verify it matches what we wrote
					value, err := getMessageFromKafka(kafkaAddress, kafkaTopic)

					Expect(err).ToNot(HaveOccurred())

					fmt.Printf("Received value: %+v\n", string(value))
				})
			})
		})

		Describe("read", func() {
			Context("plain output", func() {
				It("should work", func() {
				})
			})

			Context("protobuf output", func() {
				It("should work", func() {
				})
			})
		})
	})

	Describe("RabbitMQ", func() {
		Describe("write", func() {
			Context("plain input, plain output", func() {
				It("should work", func() {
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
				})
			})
		})

		Describe("read", func() {
			Context("plain output", func() {
				It("should work", func() {
				})
			})

			Context("protobuf output", func() {
				It("should work", func() {
				})
			})
		})
	})

	Describe("GCP PubSub", func() {
		Describe("write", func() {
			Context("plain input, plain output", func() {
				It("should work", func() {
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
				})
			})
		})

		Describe("read", func() {
			Context("plain output", func() {
				It("should work", func() {
				})
			})

			Context("protobuf output", func() {
				It("should work", func() {
				})
			})
		})
	})
})

func setupConnections(kafkaAddress, kafkaTopic, gcpPubSubProjectId, rabbitAddress string) error {
	var err error

	kafkaReader, err = newKafkaReader(kafkaAddress, kafkaTopic)
	if err != nil {
		return errors.Wrap(err, "unable to setup new kafka reader")
	}

	kafkaWriter, err = newKafkaWriter(kafkaAddress, kafkaTopic)
	if err != nil {
		return errors.Wrap(err, "unable to setup new kafka writer")
	}

	rabbitChannel, err = newRabbitChannel(rabbitAddress)
	if err != nil {
		return errors.Wrap(err, "unable to setup new rabbitmq channel")
	}

	gcpPubSubClient, err = newGCPPubSubClient(gcpPubSubProjectId)
	if err != nil {
		return errors.Wrap(err, "unable to setup new gcpPubSub client")
	}

	return nil
}

func newKafkaReader(address, topic string) (*skafka.Reader, error) {
	dialer := &skafka.Dialer{
		Timeout: 5 * time.Second,
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

	// Attempt to establish connection on startup
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", address, topic, 0); err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			address, err)
	}

	return skafka.NewReader(skafka.ReaderConfig{
		Brokers: []string{address},
		GroupID: "plumber-func-test",
		Topic:   topic,
		Dialer:  dialer,
	}), nil
}

// TODO: Implement
func newKafkaWriter(address, topic string) (*skafka.Writer, error) {
	return nil, nil
}

// TODO: Implement
func newRabbitChannel(address string) (*amqp.Channel, error) {
	return nil, nil
}

// TODO: Implement
func newGCPPubSubClient(projectId string) (*pubsub.Client, error) {
	return nil, nil
}

func getMessageFromKafka(address, topic string) ([]byte, error) {
	if kafkaReader == nil {
		return nil, errors.New("kafkaReader should not be nil")
	}

	msg, err := kafkaReader.ReadMessage(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "unable to read message from kafka")
	}

	return msg.Value, nil
}

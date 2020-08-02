// This package will perform _functional_ testing of the plumber CLI tool.
//
// It is going to perform a single, OS-specific compile via `make` and then
// re-execute the built binary over and over.
//
// NOTE: You should probably have local instances of rabbit, kafka, etc. running
// or  else the test suite will fail.
package main

import (
	"fmt"
	"math/rand"
	"os/exec"
	"runtime"

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
		kafkaAddress = "localhost:9092"
		kafkaTopic   = "test"
		binary       = "./build/plumber-" + runtime.GOOS
	)

	BeforeSuite(func() {
		if err := setupConnections(
			kafkaAddress,
			kafkaTopic,
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

// TODO: Implement
func newKafkaReader(address, topic string) (*skafka.Reader, error) {
	return nil, nil
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
	return nil, nil
}

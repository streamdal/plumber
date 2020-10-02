// +build functional

// This package will perform _functional_ testing of the plumber CLI tool.
//
// It is going to perform a single, OS-specific compile via `make` and then
// re-execute the built binary over and over.
//
// NOTE 1: You should probably have local instances of rabbit, kafka, etc. running
// or  else the test suite will fail.
//
// NOTE 2: Testing of the CLI is flakey - output is sometimes delayed due to
// slow dependencies and OS specific quirks. Due to this, the functional test
// suite is not ran on PR's and is left as something to be ran manually.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os/exec"
	"runtime"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	skafka "github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/test-assets/pbs"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	SetDefaultEventuallyTimeout(time.Second * 30)
}

var _ = Describe("Functional", func() {
	var (
		kafkaAddress         = "localhost:9092"
		binary               = "./build/plumber-" + runtime.GOOS
		sampleOutboundJSONPB = "./test-assets/messages/sample-outbound.json"
		protoSchemasDir      = "./test-assets/protos"
	)

	BeforeSuite(func() {
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
		var (
			kafka      *Kafka
			kafkaTopic = fmt.Sprintf("plumber-test-%d", rand.Int())
		)

		Describe("write", func() {
			BeforeEach(func() {
				var err error

				kafka, err = newKafka(kafkaAddress, kafkaTopic)

				Expect(err).ToNot(HaveOccurred())
			})

			Context("plain input, plain output", func() {
				It("should work", func() {
					randString := fmt.Sprintf("kafka-random-%d", rand.Int())

					cmd := exec.Command(binary, "write", "kafka", "--address", kafkaAddress,
						"--topic", kafkaTopic, "--input-data", randString)

					_, err := cmd.CombinedOutput()

					Expect(err).ToNot(HaveOccurred())

					// Read message from kafka topic, verify it's set to randString
					msg, err := kafka.Reader.ReadMessage(context.Background())
					Expect(err).ToNot(HaveOccurred())

					Expect(err).ToNot(HaveOccurred())
					Expect(string(msg.Value)).To(Equal(randString))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
					// We use "Outbound" here because it's simple
					cmd := exec.Command(binary, "write", "kafka", "--address", kafkaAddress,
						"--topic", kafkaTopic, "--input-type", "jsonpb", "--output-type", "protobuf",
						"--input-file", sampleOutboundJSONPB, "--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound")

					_, err := cmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					// Read message from kafka topic; verify it matches what we wrote
					msg, err := kafka.Reader.ReadMessage(context.Background())
					Expect(err).ToNot(HaveOccurred())

					// Verify we wrote a valid protobuf message
					outbound := &pbs.Outbound{}

					err = proto.Unmarshal(msg.Value, outbound)

					Expect(err).ToNot(HaveOccurred())

					// Verify that the values are the same
					jsonData, err := ioutil.ReadFile(sampleOutboundJSONPB)
					Expect(err).ToNot(HaveOccurred())

					jsonMap := make(map[string]string, 0)

					err = json.Unmarshal(jsonData, &jsonMap)
					Expect(err).ToNot(HaveOccurred())

					Expect(outbound.ReplayId).To(Equal(jsonMap["replay_id"]))

					// []byte is encoded as base64, so we have to encode it to
					// verify against source JSON
					encodedBlob := base64.StdEncoding.EncodeToString(outbound.Blob)

					Expect(encodedBlob).To(Equal(jsonMap["blob"]))
				})
			})
		})

		Describe("read", func() {
			var ()

			BeforeEach(func() {
			})

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

type Kafka struct {
	Dialer *skafka.Dialer
	Conn   *skafka.Conn
	Writer *skafka.Writer
	Reader *skafka.Reader
}

func newKafka(address, topic string) (*Kafka, error) {
	dialer := &skafka.Dialer{
		Timeout: 5 * time.Second,
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

	// Attempt to establish connection on startup
	conn, err := dialer.DialLeader(ctxDeadline, "tcp", address, topic, 0)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			address, err)
	}

	return &Kafka{
		Dialer: dialer,
		Conn:   conn,
		Writer: skafka.NewWriter(skafka.WriterConfig{
			Brokers: []string{address},
			Topic:   topic,
			Dialer:  dialer,
		}),
		Reader: skafka.NewReader(skafka.ReaderConfig{
			Brokers: []string{address},
			GroupID: "plumber", // This MUST match the group id in the CLI (or else we'll receive unexpected messages)
			Topic:   topic,
			Dialer:  dialer,
		}),
	}, nil
}

// TODO: Implement
func newRabbitChannel(address string) (*amqp.Channel, error) {
	return nil, nil
}

// TODO: Implement
func newGCPPubSubClient(projectId string) (*pubsub.Client, error) {
	return nil, nil
}

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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/test-assets/pbs"
)

var (
	kafkaReader     *skafka.Reader
	kafkaWriter     *skafka.Writer
	kafkaConn       *skafka.Conn
	kafkaDialer     *skafka.Dialer
	rabbitChannel   *amqp.Channel
	gcpPubSubClient *pubsub.Client
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var _ = Describe("Functional", func() {
	var (
		kafkaAddress         = "localhost:9092"
		kafkaTopic           = fmt.Sprintf("plumber-test-%d", rand.Int())
		gcpPubSubProjectId   = "projectId"
		rabbitAddress        = "amqp://localhost:5672"
		binary               = "./build/plumber-" + runtime.GOOS
		sampleOutboundJSONPB = "./test-assets/messages/sample-outbound.json"
		protoSchemasDir      = "./test-assets/protos"
	)

	BeforeSuite(func() {
		// Suppress "slow test" warning
		config.DefaultReporterConfig.SlowSpecThreshold = (5 * time.Minute).Seconds()

		if err := initConnections(
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

	AfterSuite(func() {
		// Get rid of the topic we just created
		err := kafkaConn.DeleteTopics(kafkaTopic)
		Expect(err).ToNot(HaveOccurred())
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
					// We use "Outbound" here because it's simple
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

					// Verify we wrote a valid protobuf message
					outbound := &pbs.Outbound{}

					err = proto.Unmarshal(value, outbound)

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
			Context("plain output", func() {
				It("should work", func() {
					testValue := []byte("test-value-123")

					cmdCtx, cmdCancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
					outputCh := make(chan []byte, 0)

					// Start reading in goroutine, timeout 30s
					go func() {
						defer GinkgoRecover()

						cmd := exec.CommandContext(cmdCtx, binary, "--quiet", "read", "message", "kafka",
							"--address", kafkaAddress, "--topic", kafkaTopic)

						data, err := cmd.CombinedOutput()
						if err != nil {
							cmdCancel()
							Fail(fmt.Sprintf("unable to fetch combined output for read: %s", err))
						}

						outputCh <- data
					}()

					// Perform a write
					writeCtx, _ := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))

					go func() {
						defer GinkgoRecover()

						err := kafkaWriter.WriteMessages(writeCtx, skafka.Message{
							Value: testValue,
						})

						Expect(err).ToNot(HaveOccurred())
					}()

					// Receive data or wait for timeout to hit
					var data []byte

					select {
					case data = <-outputCh:
						close(outputCh)
					case <-writeCtx.Done():
						Fail("kafka write time out")
					case <-cmdCtx.Done():
						Fail("command timed out")
					}

					// Ensure that we received the appropriate data
					Expect(strings.Contains(string(data), string(testValue)))
				})
			})

			FContext("protobuf output", func() {
				It("should work", func() {
					cmdCtx, cmdCancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
					outputCh := make(chan []byte, 0)

					go func() {
						defer GinkgoRecover()

						// TODO: Output type should be set to protobuf w/ Outbound message type
						cmd := exec.CommandContext(cmdCtx, binary, "--quiet", "read", "message", "kafka",
							"--address", kafkaAddress, "--topic", kafkaTopic)

						data, err := cmd.CombinedOutput()
						if err != nil {
							cmdCancel()
							Fail(fmt.Sprintf("unable to fetch combined output for read: %s", err))
						}

						outputCh <- data
					}()

					// Create a protobuf encoded message
					msg := &pbs.Outbound{
						ReplayId: "replay-id-1234",
						Blob:     []byte("test"),
					}

					pbData, err := proto.Marshal(msg)

					Expect(err).ToNot(HaveOccurred())

					// Write protobuf message to kafka
					writeCtx, _ := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))

					go func() {
						defer GinkgoRecover()

						err := kafkaWriter.WriteMessages(writeCtx, skafka.Message{
							Value: pbData,
						})

						Expect(err).ToNot(HaveOccurred())
					}()

					// Receive data or wait for timeout to hit
					var data []byte

					select {
					case data = <-outputCh:
						close(outputCh)
					case <-writeCtx.Done():
						Fail("kafka write time out")
					case <-cmdCtx.Done():
						Fail("command timed out")
					}

					fmt.Printf("This is our data: %s\n", string(data))
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

func initConnections(kafkaAddress, kafkaTopic, gcpPubSubProjectId, rabbitAddress string) error {
	var err error

	kafkaConn, kafkaDialer, err = newKafka(kafkaAddress, kafkaTopic)
	if err != nil {
		return errors.Wrap(err, "unable to setup new kafka")
	}

	kafkaReader = skafka.NewReader(skafka.ReaderConfig{
		Brokers: []string{kafkaAddress},
		GroupID: "plumber-test",
		Topic:   kafkaTopic,
		Dialer:  kafkaDialer,
	})

	kafkaWriter = skafka.NewWriter(skafka.WriterConfig{
		Brokers: []string{kafkaAddress},
		Topic:   kafkaTopic,
		Dialer:  kafkaDialer,
	})

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

func newKafka(address, topic string) (*skafka.Conn, *skafka.Dialer, error) {
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
		return nil, nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			address, err)
	}

	return conn, dialer, nil
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

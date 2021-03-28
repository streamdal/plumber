// This package will perform _functional_ testing of the plumber CLI tool.
//
// It is going to perform a single, OS-specific compile via `make` and then
// re-execute the built binary over and over.
//
// NOTE 1: You should probably have local instances of rabbit, kafka, etc. running
// or  else the test suite will fail.

// +build functional

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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/schemas/build/go/events"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
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
		Describe("write", func() {
			var (
				kafka      *Kafka
				kafkaTopic = fmt.Sprintf("plumber-test-%d", rand.Int())
			)

			BeforeEach(func() {
				var err error

				kafka, err = newKafkaReader(kafkaAddress, kafkaTopic)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				kafka.Reader.Close()
				kafka.Conn.DeleteTopics(kafkaTopic)
				kafka.Conn.Close()
			})

			Context("plain input, plain output", func() {
				It("should work", func() {
					randString := fmt.Sprintf("kafka-random-%d", rand.Int())

					cmd := exec.Command(binary, "write", "kafka", "--address", kafkaAddress,
						"--topic", kafkaTopic, "--input-data", randString)

					_, err := cmd.CombinedOutput()

					Expect(err).ToNot(HaveOccurred())

					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
					defer cancel()

					// Read message from kafka topic, verify it's set to randString
					msg, err := kafka.Reader.ReadMessage(ctx)
					Expect(err).ToNot(HaveOccurred())
					Expect(string(msg.Value)).To(Equal(randString))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
					// We use "Outbound" here because it's simple
					cmd := exec.Command(binary, "write", "kafka",
						"--address", kafkaAddress,
						"--topic", kafkaTopic,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound")

					_, err := cmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
					defer cancel()

					// Read message from kafka topic; verify it matches what we wrote
					msg, err := kafka.Reader.ReadMessage(ctx)
					Expect(err).ToNot(HaveOccurred())

					// Verify we wrote a valid protobuf message
					outbound := &events.Outbound{}

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
			var (
				kafka      *Kafka
				kafkaTopic string
			)

			BeforeEach(func() {
				var err error
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				kafkaTopic = fmt.Sprintf("plumber-test-%d", r.Int())

				kafka, err = newKafkaWriter(kafkaAddress, kafkaTopic)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				kafka.Writer.Close()
			})

			Context("plain output", func() {
				It("should work", func() {
					// Write data
					writtenRecords, err := writeKafkaRecords(kafka, kafkaTopic, "json", 10)

					Expect(err).ToNot(HaveOccurred())
					Expect(len(writtenRecords)).To(Equal(10))

					// UseConsumerGroup is true by default, so subsequent reads
					// should automatically increase the offset
					for _, v := range writtenRecords {
						cmd := exec.Command(binary, "read", "kafka",
							"--address", kafkaAddress,
							"--topic", kafkaTopic,
						)

						readOut, err := cmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())

						Expect(string(readOut)).To(ContainSubstring(string(v.Value)))
					}
				})
			})

			Context("read offset", func() {
				It("should work", func() {
					// Write a few records
					writtenRecords, err := writeKafkaRecords(kafka, kafkaTopic, "json", 5)

					Expect(err).ToNot(HaveOccurred())
					Expect(len(writtenRecords)).To(Equal(5))

					for i, v := range writtenRecords {
						fmt.Printf("%d: %s\n", i, string(v.Value))
					}

					// Read at random offsets
					for i := 0; i < 10; i++ {
						randOffset := rand.Intn(5)

						cmd := exec.Command(binary, "read", "kafka",
							"--address", kafkaAddress,
							"--topic", kafkaTopic,
							"--no-use-consumer-group",
							"--read-offset", fmt.Sprintf("%d", randOffset),
						)

						readOut, err := cmd.CombinedOutput()
						if err != nil {
							Fail("failed to read: " + string(readOut))
						}

						Expect(string(readOut)).To(ContainSubstring(string(writtenRecords[randOffset].Value)))
					}
				})
			})
		})
	})

	Describe("RabbitMQ", func() {
		Describe("read/write", func() {
			Context("plain input, plain output", func() {

				randID := rand.Int()
				var (
					exchangeName string = fmt.Sprintf("testex-%d", randID)
					queueName    string = fmt.Sprintf("testqueue-%d", randID)
					routingKey   string = fmt.Sprintf("testqueue-%d", randID)
				)

				BeforeEach(func() {
					err := createRabbit(exchangeName, queueName, routingKey)
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					err := deleteRabbit(exchangeName, queueName)
					Expect(err).ToNot(HaveOccurred())
				})

				It("should work", func() {
					const testMessage string = "welovemessaging"

					// First write the message to Rabbit
					writeCmd := exec.Command(
						binary,
						"write",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to exchange '%s'", exchangeName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					time.Sleep(time.Second * 1)

					// Now try and read from the RabbitMQ queue
					readCmd := exec.CommandContext(
						ctx,
						binary,
						"read",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--queue", queueName,
					)

					readOutput, err := readCmd.CombinedOutput()
					if err != nil {
						Fail("read failed: " + string(readOutput))
					}

					if ctx.Err() == context.DeadlineExceeded {
						Fail("Rabbit plaintext read failed")
					}

					readGot := string(readOutput[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				randID := rand.Int()
				var (
					exchangeName string = fmt.Sprintf("testex-%d", randID)
					queueName    string = fmt.Sprintf("testqueue-%d", randID)
					routingKey   string = fmt.Sprintf("testqueue-%d", randID)
				)

				BeforeEach(func() {
					err := createRabbit(exchangeName, queueName, routingKey)
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					err := deleteRabbit(exchangeName, queueName)
					Expect(err).ToNot(HaveOccurred())
				})

				It("should work", func() {
					writeCmd := exec.Command(
						binary,
						"write",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to exchange '%s'", exchangeName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					// Now try and read from the RabbitMQ queue
					readCmd := exec.CommandContext(
						ctx,
						binary,
						"read",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--queue", queueName,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					readOut, err := readCmd.CombinedOutput()
					if err != nil {
						Fail("read failed: " + string(readOut))
					}

					if ctx.Err() == context.DeadlineExceeded {
						Fail("Rabbit protobuf read failed")
					}

					readGot := string(readOut[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("eyJoZWxsbyI6ImRhbiJ9Cg=="))
				})
			})
		})

		Context("avro and json", func() {

			randID := rand.Int()
			var (
				exchangeName string = fmt.Sprintf("testex-%d", randID)
				queueName    string = fmt.Sprintf("testqueue-%d", randID)
				routingKey   string = fmt.Sprintf("testqueue-%d", randID)
			)

			BeforeEach(func() {
				err := createRabbit(exchangeName, queueName, routingKey)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := deleteRabbit(exchangeName, queueName)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should work", func() {
				const testMessage string = "{\"company\":\"Batch Corp\"}"

				// First write the message to Rabbit
				writeCmd := exec.Command(
					binary,
					"write",
					"rabbit",
					"--avro-schema", "./test-assets/avro/test.avsc",
					"--exchange", exchangeName,
					"--routing-key", routingKey,
					"--input-data", testMessage,
				)

				writeOut, err := writeCmd.CombinedOutput()
				if err != nil {
					Fail("write failed: " + string(writeOut))
				}

				writeGot := string(writeOut[:])
				writeWant := fmt.Sprintf("Successfully wrote message to exchange '%s'", exchangeName)
				Expect(writeGot).To(ContainSubstring(writeWant))

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				// Now try and read from the RabbitMQ queue
				readCmd := exec.CommandContext(
					ctx,
					binary,
					"read",
					"rabbit",
					"--avro-schema", "./test-assets/avro/test.avsc",
					"--exchange", exchangeName,
					"--routing-key", routingKey,
					"--queue", queueName,
				)

				readOutput, err := readCmd.CombinedOutput()
				if err != nil {
					Fail("read failed: " + string(readOutput))
				}

				if ctx.Err() == context.DeadlineExceeded {
					Fail("Rabbit AVRO read failed")
				}

				readGot := string(readOutput[:])
				Expect(readGot).To(ContainSubstring(testMessage))
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

	Describe("AWS SQS", func() {

		Describe("read/write", func() {
			var queueName string

			BeforeEach(func() {
				queueName = fmt.Sprintf("FunctionalTestQueue%d", rand.Int())
				err := createSqsQueue(queueName)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := deleteSqsQueue(queueName)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					// First write the message to SQS
					writeCmd := exec.Command(
						binary,
						"write",
						"aws-sqs",
						"--queue-name", queueName,
						"--input-data", testMessage,
					)

					writeOut, _ := writeCmd.CombinedOutput()
					//Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to AWS queue '%s'", queueName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					// Now try and read from the SQS queue
					readCmd := exec.Command(
						binary,
						"read",
						"aws-sqs",
						"--queue-name", queueName,
						"--auto-delete",
					)

					readOutput, _ := readCmd.CombinedOutput()
					//Expect(err).ToNot(HaveOccurred())

					readGot := string(readOutput[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
					writeCmd := exec.Command(
						binary,
						"write",
						"aws-sqs",
						"--queue-name", queueName,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to AWS queue '%s'", queueName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					readCmd := exec.Command(
						binary,
						"read",
						"aws-sqs",
						"--queue-name", queueName,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					readOut, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					readGot := string(readOut[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("eyJoZWxsbyI6ImRhbiJ9Cg=="))
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					// First write the message to SQS
					writeCmd := exec.Command(
						binary,
						"write",
						"aws-sqs",
						"--queue-name", queueName,
						"--input-data", testMessage,
						"--avro-schema", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to AWS queue '%s'", queueName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					// Now try and read from the SQS queue
					readCmd := exec.Command(
						binary,
						"read",
						"aws-sqs",
						"--queue-name", queueName,
						"--auto-delete",
						"--avro-schema", "./test-assets/avro/test.avsc",
					)

					readOutput, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					readGot := string(readOutput[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})
		})
	})

	Describe("Mosquitto", func() {

		Describe("read/write", func() {
			var topicName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan []byte)

					// Start MQTT reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"mqtt",
							"--topic", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// Reader is ready, write the message to MQTT
					writeCmd := exec.Command(
						binary,
						"write",
						"mqtt",
						"--topic", topicName,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Sending message to broker on topic '%s'", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan []byte)

					// Start MQTT reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"mqtt",
							"--topic", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// Reader is ready, write the message to MQTT
					writeCmd := exec.Command(
						binary,
						"write",
						"mqtt",
						"--topic", topicName,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Sending message to broker on topic '%s'", topicName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan []byte)

					// Start MQTT reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"mqtt",
							"--topic", topicName,
							"--avro-schema", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to MQTT
					writeCmd := exec.Command(
						binary,
						"write",
						"mqtt",
						"--topic", topicName,
						"--input-data", testMessage,
						"--avro-schema", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Sending message to broker on topic '%s'", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})
		})
	})

	Describe("ActiveMQ", func() {
		Describe("read/write", func() {
			Context("plain input, plain output", func() {
				It("should work", func() {
					var queueName string = fmt.Sprintf("TestQueue%d", rand.Int())
					const testMessage string = "welovemessaging"

					// First write the message
					writeCmd := exec.Command(
						binary,
						"write",
						"activemq",
						"--queue", queueName,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", queueName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					// Now try and read from the queue
					readCmd := exec.Command(
						binary,
						"read",
						"activemq",
						"--queue", queueName,
					)

					readOutput, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					readGot := string(readOutput[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
					var queueName string = fmt.Sprintf("TestQueue%d", rand.Int())

					writeCmd := exec.Command(
						binary,
						"write",
						"activemq",
						"--queue", queueName,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", queueName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					// Now try and read from the queue
					readCmd := exec.Command(
						binary,
						"read",
						"activemq",
						"--queue", queueName,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					readOut, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					readGot := string(readOut[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("eyJoZWxsbyI6ImRhbiJ9Cg=="))
				})
			})
		})

		Context("avro and json", func() {
			It("should work", func() {
				var queueName string = fmt.Sprintf("TestQueue%d", rand.Int())
				const testMessage string = "{\"company\":\"Batch Corp\"}"

				// First write the message
				writeCmd := exec.Command(
					binary,
					"write",
					"activemq",
					"--avro-schema", "./test-assets/avro/test.avsc",
					"--queue", queueName,
					"--input-data", testMessage,
				)

				writeOut, err := writeCmd.CombinedOutput()
				Expect(err).ToNot(HaveOccurred())

				writeGot := string(writeOut[:])
				writeWant := fmt.Sprintf("Successfully wrote message to '%s'", queueName)
				Expect(writeGot).To(ContainSubstring(writeWant))

				// Now try and read from the queue
				readCmd := exec.Command(
					binary,
					"read",
					"activemq",
					"--avro-schema", "./test-assets/avro/test.avsc",
					"--queue", queueName,
				)

				readOutput, err := readCmd.CombinedOutput()
				Expect(err).ToNot(HaveOccurred())

				readGot := string(readOutput[:])
				Expect(readGot).To(ContainSubstring(testMessage))
			})
		})
	})

	Describe("NATS", func() {

		Describe("read/write", func() {
			var topicName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan []byte)

					// Start NATS reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats",
							"--subject", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// Reader is ready, write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"nats",
						"--subject", topicName,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan []byte)

					// Start NATS reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats",
							"--subject", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// Reader is ready, write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"nats",
						"--subject", topicName,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", topicName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan []byte)

					// Start NATS reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats",
							"--subject", topicName,
							"--avro-schema", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"nats",
						"--subject", topicName,
						"--input-data", testMessage,
						"--avro-schema", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})
		})
	})

	FDescribe("RedisPubSub PubSub", func() {
		Describe("read/write", func() {
			var topicName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan []byte)

					// Start RedisPubSub reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-pubsub",
							"--channels", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// Reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-pubsub",
						"--channels", topicName,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan []byte)

					// Start RedisPubSub reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-pubsub",
							"--channels", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// Reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-pubsub",
						"--channels", topicName,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", topicName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan []byte)

					// Start RedisPubSub reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-pubsub",
							"--channels", topicName,
							"--avro-schema", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-pubsub",
						"--channels", topicName,
						"--input-data", testMessage,
						"--avro-schema", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to '%s'", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})
		})
	})

	Describe("RedisPubSub Streams", func() {
		Describe("read/write", func() {
			var topicName string
			var keyName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
				keyName = fmt.Sprintf("FunctionalTestKey%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan []byte)

					// Start RedisPubSub reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-streams",
							"--streams", topicName,
							"--create-streams",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// Reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-streams",
						"--key", keyName,
						"--streams", topicName,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to stream '%s' with key", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan []byte)

					// Start RedisPubSub reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-streams",
							"--streams", topicName,
							"--create-streams",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// Reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-streams",
						"--streams", topicName,
						"--key", keyName,
						"--input-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to stream '%s' with key", topicName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan []byte)

					// Start RedisPubSub reader command
					go func() {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-streams",
							"--streams", topicName,
							"--avro-schema", "./test-assets/avro/test.avsc",
							"--create-streams",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						capture <- readOutput
					}()

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-streams",
						"--streams", topicName,
						"--key", keyName,
						"--input-data", testMessage,
						"--avro-schema", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut[:])

					writeWant := fmt.Sprintf("Successfully wrote message to stream '%s' with key", topicName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					output := <-capture
					close(capture)

					readGot := string(output[:])
					Expect(readGot).To(ContainSubstring(testMessage))
				})
			})
		})
	})
})

type Kafka struct {
	Dialer *skafka.Dialer
	Conn   *skafka.Conn
	Reader *skafka.Reader
	Writer *skafka.Writer
}

func newKafkaReader(address, topic string) (*Kafka, error) {
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
		Reader: skafka.NewReader(skafka.ReaderConfig{
			Brokers:          []string{address},
			GroupID:          "plumber", // This MUST match the group id in the CLI (or else we'll receive unexpected messages)
			Topic:            topic,
			Dialer:           dialer,
			RebalanceTimeout: 0,
			MaxWait:          time.Millisecond * 50,
			MaxBytes:         1048576,
			QueueCapacity:    1,
		}),
	}, nil
}

func newKafkaWriter(address, topic string) (*Kafka, error) {
	dialer := &skafka.Dialer{
		Timeout: 5 * time.Second,
	}

	fmt.Println("Creating a new writer for topic: ", topic)

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

	// Broker should auto create topics - let's do this just in case
	if err := conn.CreateTopics(skafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		return nil, errors.Wrap(err, "unable to create topic")
	}

	// Give the broker some time to become aware of the topic
	time.Sleep(5 * time.Second)

	return &Kafka{
		Dialer: dialer,
		Conn:   conn,
		Writer: &skafka.Writer{
			Addr:  skafka.TCP(address),
			Topic: topic,
		},
	}, nil
}

func writeKafkaRecords(client *Kafka, topic, dataType string, num int) ([]skafka.Message, error) {
	if client == nil {
		return nil, errors.New("received a nil kafka client")
	}

	var messages []skafka.Message

	for i := 0; i < num; i++ {
		var entry []byte

		switch dataType {
		case "json":
			entry = []byte(fmt.Sprintf(`{"foo":"bar-%d"}`, rand.Intn(10000)))
		case "protobuf":
			return nil, errors.New("not implemented")
		case "plain":
			entry = []byte(fmt.Sprintf("foo-%d", rand.Intn(10000)))
		default:
			return nil, fmt.Errorf("unknown data type '%s'", dataType)
		}

		messages = append(messages, skafka.Message{
			Topic: topic,
			Value: entry,
		})
	}

	if err := client.Writer.WriteMessages(context.Background(), messages...); err != nil {
		return nil, fmt.Errorf("unable to write message: %s", err)
	}

	return messages, nil
}

// TODO: Implement
func newGCPPubSubClient(projectId string) (*pubsub.Client, error) {
	return nil, nil
}

// createSqsQueue creates a new transient queue we will use for testing
func createSqsQueue(name string) error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	_, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})

	return err
}

// deleteSqsQueue deletes a transient queue used for testing
func deleteSqsQueue(name string) error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	// Delete requires the full queue URL
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})

	if err != nil {
		return err
	}

	_, err = svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: resultURL.QueueUrl,
	})

	return err
}

func createRabbit(exchangeName, queueName, routingKey string) error {
	var err error
	cmd := exec.Command("docker", "exec", "rabbitmq", "rabbitmqadmin", "declare", "exchange", "name="+exchangeName, "type=topic")
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}

	cmd = exec.Command("docker", "exec", "rabbitmq", "rabbitmqadmin", "declare", "queue", "name="+queueName, "durable=false")
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	cmd = exec.Command("docker", "exec", "rabbitmq", "rabbitmqadmin", "declare", "binding", "source="+exchangeName, "destination="+queueName, "routing_key="+routingKey)
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	return nil
}

func deleteRabbit(exchangeName, queueName string) error {
	var err error
	cmd := exec.Command("docker", "exec", "rabbitmq", "rabbitmqadmin", "delete", "exchange", "name="+exchangeName)
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	cmd = exec.Command("docker", "exec", "rabbitmq", "rabbitmqadmin", "delete", "queue", "name="+queueName)
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	return nil
}

// This package will perform _functional_ testing of the plumber CLI tool.
//
// It is going to perform a single, OS-specific compile via `make` and then
// re-execute the built binary over and over.
//
// NOTE 1: You should probably have local instances of rabbit, kafka, etc. running
// or  else the test suite will fail.

//go:build functional
// +build functional

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
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
						"--topics", kafkaTopic, "--input", randString)

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
						"--topics", kafkaTopic,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound")

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
					writtenRecords, err := writeKafkaRecords(kafka, kafkaTopic, "json", 1)

					Expect(err).ToNot(HaveOccurred())
					Expect(len(writtenRecords)).To(Equal(1))

					// UseConsumerGroup is true by default, so subsequent reads
					// should automatically increase the offset
					for _, v := range writtenRecords {
						cmd := exec.Command(binary, "read", "kafka",
							"--address", kafkaAddress,
							"--topics", kafkaTopic,
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
							"--topics", kafkaTopic,
							"--use-consumer-group=false",
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

			Context("thrift decoding", func() {
				XIt("should work", func() {
					// First write the message to Rabbit
					writeCmd := exec.Command(
						binary,
						"write",
						"kafka",
						"--topics", kafkaTopic,
						"--input-file", "test-assets/thrift/test_message.bin",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					time.Sleep(time.Second * 1)

					// Now try and read from the RabbitMQ queue
					readCmd := exec.CommandContext(
						ctx,
						binary,
						"read",
						"kafka",
						"--topics", kafkaTopic,
						"--decode-type", "thrift",
					)

					readOutput, err := readCmd.CombinedOutput()
					if err != nil {
						Fail("read failed: " + string(readOutput))
					}

					if ctx.Err() == context.DeadlineExceeded {
						Fail("Kafka thrift read failed")
					}

					readGot := string(readOutput)
					Expect(readGot).To(ContainSubstring(`{"1":"submessage value here"}`))
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
						"--exchange-name", exchangeName,
						"--routing-key", routingKey,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"
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
						"--exchange-name", exchangeName,
						"--binding-key", routingKey,
						"--queue-name", queueName,
					)

					readOutput, err := readCmd.CombinedOutput()
					if err != nil {
						Fail("read failed: " + string(readOutput))
					}

					if ctx.Err() == context.DeadlineExceeded {
						Fail("Rabbit plaintext read failed")
					}

					readGot := string(readOutput)
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
						"--exchange-name", exchangeName,
						"--routing-key", routingKey,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					// Now try and read from the RabbitMQ queue
					readCmd := exec.CommandContext(
						ctx,
						binary,
						"read",
						"rabbit",
						"--exchange-name", exchangeName,
						"--binding-key", routingKey,
						"--queue-name", queueName,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					readOut, err := readCmd.CombinedOutput()
					if err != nil {
						Fail("read failed: " + string(readOut))
					}

					if ctx.Err() == context.DeadlineExceeded {
						Fail("Rabbit protobuf read failed")
					}

					readGot := string(readOut)
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					//Expect(readGot).To(ContainSubstring("eyJoZWxsbyI6ImRhbiJ9Cg=="))
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
					"--avro-schema-file", "./test-assets/avro/test.avsc",
					"--exchange-name", exchangeName,
					"--routing-key", routingKey,
					"--input", testMessage,
				)

				writeOut, err := writeCmd.CombinedOutput()
				if err != nil {
					Fail("write failed: " + string(writeOut))
				}

				writeGot := string(writeOut)
				writeWant := "Successfully wrote '1' message(s)"
				Expect(writeGot).To(ContainSubstring(writeWant))

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				// Now try and read from the RabbitMQ queue
				readCmd := exec.CommandContext(
					ctx,
					binary,
					"read",
					"rabbit",
					"--avro-schema-file", "./test-assets/avro/test.avsc",
					"--exchange-name", exchangeName,
					"--binding-key", routingKey,
					"--queue-name", queueName,
				)

				readOutput, err := readCmd.CombinedOutput()
				if err != nil {
					Fail("read failed: " + string(readOutput))
				}

				if ctx.Err() == context.DeadlineExceeded {
					Fail("Rabbit AVRO read failed")
				}

				readGot := string(readOutput)
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

	XDescribe("AWS SQS", func() {

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
						"--input", testMessage,
					)

					writeOut, _ := writeCmd.CombinedOutput()
					//Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
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

					readGot := string(readOutput)
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
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					readCmd := exec.Command(
						binary,
						"read",
						"aws-sqs",
						"--queue-name", queueName,
						"--decode-type", "protobuf",
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					readOut, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					readGot := string(readOut)
					Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
					//Expect(readGot).To(ContainSubstring("eyJoZWxsbyI6ImRhbiJ9Cg=="))
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
						"--input", testMessage,
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					// Now try and read from the SQS queue
					readCmd := exec.Command(
						binary,
						"read",
						"aws-sqs",
						"--queue-name", queueName,
						"--auto-delete",
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					readOutput, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					readGot := string(readOutput)
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

					capture := make(chan string, 1)
					defer close(capture)

					// Run MQTT reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"mqtt",
							"--topic", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Second * 1)

					// reader is ready, write the message to MQTT
					writeCmd := exec.Command(
						binary,
						"write",
						"mqtt",
						"--topic", topicName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for mqtt message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan string, 1)

					// Run MQTT reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"mqtt",
							"--topic", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Second * 1)

					// reader is ready, write the message to MQTT
					writeCmd := exec.Command(
						binary,
						"write",
						"mqtt",
						"--topic", topicName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
					case <-ctx.Done():
						Fail("timed out waiting for mqtt message")
					}
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan string, 1)
					defer close(capture)

					// Run MQTT reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"mqtt",
							"--topic", topicName,
							"--avro-schema-file", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Second * 1)

					// First write the message to MQTT
					writeCmd := exec.Command(
						binary,
						"write",
						"mqtt",
						"--topic", topicName,
						"--input", testMessage,
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for mqtt message")
					}
				})
			})
		})
	})

	XDescribe("ActiveMQ", func() {
		Describe("read/write", func() {
			var queueName string

			BeforeEach(func() {
				queueName = fmt.Sprintf("TestQueue%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan string, 1)
					defer close(capture)

					// Run activemq reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"activemq",
							"--queue", queueName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 500)

					// reader is ready, write the message to ActiveMQ
					writeCmd := exec.Command(
						binary,
						"write",
						"activemq",
						"--queue", queueName,
						"--input", testMessage,
					)

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					time.Sleep(time.Second * 1)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for activeMQ message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan string, 1)
					defer close(capture)

					// Run ActiveMQ reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"activemq",
							"--queue", queueName,
							"--decode-type", "protobuf",
							"--protobuf-dirs", protoSchemasDir,
							"--protobuf-root-message", "events.Outbound",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 500)

					// reader is ready, write the message to ActiveMQ
					writeCmd := exec.Command(
						binary,
						"write",
						"activemq",
						"--queue", queueName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					time.Sleep(time.Second * 1)

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("eyJoZWxsbyI6ImRhbiJ9Cg=="))
					case <-ctx.Done():
						Fail("timed out waiting for activeMQ message")
					}
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan string, 1)
					defer close(capture)

					// Run ActiveMQ reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"activemq",
							"--avro-schema-file", "./test-assets/avro/test.avsc",
							"--queue", queueName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 500)

					// First write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"activemq",
						"--avro-schema-file", "./test-assets/avro/test.avsc",
						"--queue", queueName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					time.Sleep(time.Second * 1)

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for activeMQ message")
					}
				})
			})
		})
	})

	XDescribe("NATS", func() {

		Describe("read/write", func() {
			var topicName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan string, 1)
					defer close(capture)

					// Run NATS reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats",
							"--subject", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"nats",
						"--subject", topicName,
						"--input", testMessage,
					)

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for nats message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan string, 1)
					defer close(capture)

					// Run NATS reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats",
							"--subject", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// reader is ready, write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"nats",
						"--subject", topicName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
					case <-ctx.Done():
						Fail("timed out waiting for nats message")
					}
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan string, 1)
					defer close(capture)

					// Run NATS reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats",
							"--subject", topicName,
							"--avro-schema-file", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to NATS
					writeCmd := exec.Command(
						binary,
						"write",
						"nats",
						"--subject", topicName,
						"--input", testMessage,
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for nats message")
					}
				})
			})
		})
	})

	Describe("NATS Jetstream", func() {
		Describe("read/write", func() {
			var streamName string

			BeforeEach(func() {
				streamName = fmt.Sprintf("FunctionalTestSteam-%d", rand.Int())
				err := createNatsJsStream(streamName)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("plain input, plain output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan string, 1)
					defer close(capture)

					// Run Jetstream reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats-jetstream",
							"--stream", streamName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"nats-jetstream",
						"--stream", streamName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					time.Sleep(time.Millisecond * 1000)

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for nats-jetstream message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {
					capture := make(chan string, 1)
					defer close(capture)
					defer GinkgoRecover()

					// Run Jetstream reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"nats-jetstream",
							"--stream", streamName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					writeCmd := exec.Command(
						binary,
						"write",
						"nats-jetstream",
						"--stream", streamName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
					case <-ctx.Done():
						Fail("timed out waiting for natsjs message")
					}
				})
			})

		})

		// XDescribe does not work nested so this locally passed test is commented out
		//Describe("avro-json read/write", func() {
		//	Context("avro and json", func() {
		//		defer GinkgoRecover()
		//
		//		streamName := fmt.Sprintf("FunctionalTestSteam-%d", rand.Int())
		//		err := createNatsJsStream(streamName)
		//		const testMessage string = "{\"company\":\"Batch Corp\"}"
		//
		//		capture := make(chan string, 1)
		//		defer close(capture)
		//
		//		// Run Jetstream reader command
		//		go func(out chan string) {
		//			defer GinkgoRecover()
		//
		//			readCmd := exec.Command(
		//				binary,
		//				"read",
		//				"nats-jetstream",
		//				"--stream", streamName,
		//				"--avro-schema-file", "./test-assets/avro/test.avsc",
		//			)
		//
		//			readOutput, err := readCmd.CombinedOutput()
		//			Expect(err).ToNot(HaveOccurred())
		//			out <- string(readOutput)
		//		}(capture)
		//
		//		// Wait for reader to start up
		//		time.Sleep(time.Millisecond * 100)
		//
		//		// reader is ready
		//		writeCmd := exec.Command(
		//			binary,
		//			"write",
		//			"nats-jetstream",
		//			"--stream", streamName,
		//			"--input", testMessage,
		//			"--avro-schema-file", "./test-assets/avro/test.avsc",
		//		)
		//
		//		writeOut, err := writeCmd.CombinedOutput()
		//		Expect(err).ToNot(HaveOccurred())
		//
		//		writeGot := string(writeOut)
		//
		//		writeWant := "Successfully wrote '1' message(s)"
		//		Expect(writeGot).To(ContainSubstring(writeWant))
		//
		//		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		//		defer cancel()
		//		time.Sleep(time.Millisecond * 1000)
		//
		//		select {
		//		case readGot := <-capture:
		//			Expect(readGot).To(ContainSubstring(testMessage))
		//		case <-ctx.Done():
		//			Fail("timed out waiting for nats-jetstream message")
		//		}
		//	})
		//})
	})

	Describe("RedisPubSub PubSub", func() {
		Describe("read/write", func() {
			var topicName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("should work", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan string, 1)
					defer close(capture)

					// Run RedisPubSub reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-pubsub",
							"--channels", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-pubsub",
						"--channels", topicName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					time.Sleep(time.Millisecond * 100)

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for redis-pubsub message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan string, 1)
					defer close(capture)

					// Run RedisPubSub reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-pubsub",
							"--channels", topicName,
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-pubsub",
						"--channels", topicName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
					case <-ctx.Done():
						Fail("timed out waiting for redis-pubsub message")
					}
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan string, 1)
					defer close(capture)

					// Run RedisPubSub reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-pubsub",
							"--channels", topicName,
							"--avro-schema-file", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-pubsub",
						"--channels", topicName,
						"--input", testMessage,
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for redis-pubsub message")
					}
				})
			})
		})
	})

	Describe("Redis Streams", func() {
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

					capture := make(chan string, 1)
					defer close(capture)

					// Run RedisPubSub reader command
					go func(out chan string) {
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
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-streams",
						"--key", keyName,
						"--streams", topicName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					time.Sleep(time.Millisecond * 1000)

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for redis-streams message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan string, 1)

					// Run RedisPubSub reader command
					go func(out chan string) {
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
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-streams",
						"--streams", topicName,
						"--key", keyName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
					case <-ctx.Done():
						Fail("timed out waiting for redis-streams message")
					}
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan string, 1)
					defer close(capture)

					// Run RedisPubSub reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"redis-streams",
							"--streams", topicName,
							"--avro-schema-file", "./test-assets/avro/test.avsc",
							"--create-streams",
						)

						readOutput, err := readCmd.CombinedOutput()
						Expect(err).ToNot(HaveOccurred())
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"redis-streams",
						"--streams", topicName,
						"--key", keyName,
						"--input", testMessage,
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for redis-streams message")
					}
				})
			})
		})
	})

	XDescribe("Apache Pulsar", func() {
		Describe("read/write", func() {
			var topicName string

			BeforeEach(func() {
				topicName = fmt.Sprintf("FunctionalTestTopic%d", rand.Int())
			})

			Context("plain input and output", func() {
				It("reads using a subscription", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan string, 1)
					defer close(capture)

					// Run Pulsar reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"pulsar",
							"--topic", topicName,
							"--subscription-name", "plumber",
						)

						readOutput, err := readCmd.CombinedOutput()
						if err != nil {
							Fail("read failed: " + string(readOutput))
						}
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"pulsar",
						"--topic", topicName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for pulsar message")
					}
				})

				It("reads using reader", func() {
					const testMessage string = "welovemessaging"

					capture := make(chan string, 1)

					// Run Pulsar reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"pulsar",
							"--topic", topicName,
							"--subscription-name", "plumber",
						)

						readOutput, err := readCmd.CombinedOutput()
						if err != nil {
							Fail("read failed: " + string(readOutput))
						}
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"pulsar",
						"--topic", topicName,
						"--input", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for pulsar message")
					}
				})
			})

			Context("jsonpb input, protobuf output", func() {
				It("should work", func() {

					capture := make(chan string, 1)
					defer close(capture)

					// Run Pulsar reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"pulsar",
							"--topic", topicName,
							"--subscription-name", "plumber",
						)

						readOutput, err := readCmd.CombinedOutput()
						if err != nil {
							Fail("read failed: " + string(readOutput))
						}
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 100)

					// reader is ready, write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"pulsar",
						"--topic", topicName,
						"--encode-type", "jsonpb",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dirs", protoSchemasDir,
						"--protobuf-root-message", "events.Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)
					writeWant := "Successfully wrote '1' message(s)"

					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring("30ddb850-1aca-4ee5-870c-1bb7b339ee5d"))
						Expect(readGot).To(ContainSubstring("{\"hello\":\"dan\"}"))
					case <-ctx.Done():
						Fail("timed out waiting for pulsar message")
					}
				})
			})

			Context("avro and json", func() {
				It("should work", func() {
					const testMessage string = "{\"company\":\"Batch Corp\"}"

					capture := make(chan string, 1)
					defer close(capture)

					// Run Pulsar reader command
					go func(out chan string) {
						defer GinkgoRecover()

						readCmd := exec.Command(
							binary,
							"read",
							"pulsar",
							"--topic", topicName,
							"--subscription-name", "plumber",
							"--avro-schema-file", "./test-assets/avro/test.avsc",
						)

						readOutput, err := readCmd.CombinedOutput()
						if err != nil {
							Fail("read failed: " + string(readOutput))
						}
						out <- string(readOutput)
					}(capture)

					// Wait for reader to start up
					time.Sleep(time.Millisecond * 50)

					// First write the message to RedisPubSub
					writeCmd := exec.Command(
						binary,
						"write",
						"pulsar",
						"--topic", topicName,
						"--input", testMessage,
						"--avro-schema-file", "./test-assets/avro/test.avsc",
					)

					writeOut, err := writeCmd.CombinedOutput()
					if err != nil {
						Fail("write failed: " + string(writeOut))
					}

					writeGot := string(writeOut)

					writeWant := "Successfully wrote '1' message(s)"
					Expect(writeGot).To(ContainSubstring(writeWant))

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					select {
					case readGot := <-capture:
						Expect(readGot).To(ContainSubstring(testMessage))
					case <-ctx.Done():
						Fail("timed out waiting for pulsar message")
					}
				})
			})
		})
	})

	Describe("manage subcommand", func() {
		Describe("kafka", func() {
			Describe("relay", func() {
				var connId string
				var relayId string

				It("create should work", func() {
					connId = createKafkaConnection(binary)
					type createRelayResp struct {
						RelayId string `json:"relayId"`
						Status  struct {
							Message   string `json:"message"`
							RequestId string `json:"requestId"`
						} `json:"status"`
					}
					testCollectionToken := os.Getenv("TEST_COLLECTION_TOKEN")
					if testCollectionToken == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token")
					}

					createRelayCmd := exec.Command(
						binary,
						"manage",
						"create",
						"relay",
						"kafka",
						"--batchsh-grpc-address", "grpc-collector.dev.batch.sh:9000",
						"--connection-id", connId,
						"--collection-token", testCollectionToken,
						"--topics", "foo",
					)
					out, err := createRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					resp := createRelayResp{}
					json.Unmarshal(out, &resp)
					Expect(string(out)).To(ContainSubstring("Relay started"))

					relayId = resp.RelayId
				})

				It("get should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					type getRelayResp struct {
						Opts struct {
							RelayId string `json:"RelayId"`
						}
					}
					getRelayCmd := exec.Command(
						binary,
						"manage",
						"get",
						"relay",
						"--id", relayId,
					)
					out, err := getRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					resp := getRelayResp{}
					json.Unmarshal(out, &resp)

					Expect(resp.Opts.RelayId).To(Equal(relayId))
				})

				It("stop should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					stopRelayCmd := exec.Command(
						binary,
						"manage",
						"stop",
						"relay",
						"--id", relayId,
					)
					out, err := stopRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Relay stopped"))
				})

				It("resume should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					resumeRelayCmd := exec.Command(
						binary,
						"manage",
						"resume",
						"relay",
						"--id", relayId,
					)
					out, err := resumeRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Relay resumed"))
				})

				It("delete should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					deleteRelayCmd := exec.Command(
						binary,
						"manage",
						"delete",
						"relay",
						"--id", relayId,
					)
					out, err := deleteRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Relay deleted"))

					// delete connection
					deleteConnection(binary, connId)
				})
			})

			Describe("tunnel", func() {
				var connId string
				var tunnelId string

				It("create should work", func() {
					connId = createKafkaConnection(binary)
					type createTunnelResp struct {
						Status struct {
							Message   string `json:"message"`
							RequestId string `json:"requestId"`
						} `json:"status"`
						TunnelId string `json:"tunnelId"`
					}

					batchAPIToken := os.Getenv("TEST_API_TOKEN")
					if batchAPIToken == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no API token")
					}

					createTunnelCmd := exec.Command(
						binary,
						"manage",
						"create",
						"tunnel",
						"kafka",
						"--x-tunnel-address", "dproxy.dev.batch.sh:443",
						"--connection-id", connId,
						"--tunnel-token", batchAPIToken,
						"--topics", "foo",
					)
					out, err := createTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					resp := createTunnelResp{}
					json.Unmarshal(out, &resp)
					Expect(string(out)).To(ContainSubstring("Tunnel created"))

					tunnelId = resp.TunnelId
				})

				It("get should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					type getTunnelResp struct {
						Opts struct {
							TunnelId string `json:"TunnelId"`
						}
					}
					getTunnelCmd := exec.Command(
						binary,
						"manage",
						"get",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := getTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					resp := getTunnelResp{}
					json.Unmarshal(out, &resp)

					Expect(resp.Opts.TunnelId).To(Equal(tunnelId))
				})

				It("stop should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					stopTunnelCmd := exec.Command(
						binary,
						"manage",
						"stop",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := stopTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Tunnel replay stopped"))
				})

				It("resume should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					resumeTunnelCmd := exec.Command(
						binary,
						"manage",
						"resume",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := resumeTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Tunnel replay resumed"))
				})

				It("delete should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					deleteTunnelCmd := exec.Command(
						binary,
						"manage",
						"delete",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := deleteTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Tunnel replay deleted"))

					// delete connection
					deleteConnection(binary, connId)
				})

			})

		})

		Describe("rabbit", func() {
			Describe("relay", func() {
				var connId string
				var relayId string

				It("create should work", func() {
					connId = createRabbitConnection(binary)

					randID := rand.Int()
					var (
						exchangeName string = fmt.Sprintf("testex-%d", randID)
						queueName    string = fmt.Sprintf("testqueue-%d", randID)
						routingKey   string = fmt.Sprintf("testqueue-%d", randID)
					)
					err := createRabbit(exchangeName, queueName, routingKey)
					Expect(err).ToNot(HaveOccurred())

					type createRelayResp struct {
						RelayId string `json:"relayId"`
						Status  struct {
							Message   string `json:"message"`
							RequestId string `json:"requestId"`
						} `json:"status"`
					}
					testCollectionToken := os.Getenv("TEST_COLLECTION_TOKEN")
					if testCollectionToken == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token")
					}

					createRelayCmd := exec.Command(
						binary,
						"manage",
						"create",
						"relay",
						"rabbit",
						"--batchsh-grpc-address", "grpc-collector.dev.batch.sh:9000",
						"--connection-id", connId,
						"--collection-token", testCollectionToken,
						"--exchange-name", exchangeName,
						"--queue-name", queueName,
						"--binding-key", routingKey,
					)
					out, err := createRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					resp := createRelayResp{}
					json.Unmarshal(out, &resp)
					Expect(string(out)).To(ContainSubstring("Relay started"))

					relayId = resp.RelayId
				})

				It("get should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					type getRelayResp struct {
						Opts struct {
							RelayId string `json:"RelayId"`
						}
					}
					getRelayCmd := exec.Command(
						binary,
						"manage",
						"get",
						"relay",
						"--id", relayId,
					)
					out, err := getRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					resp := getRelayResp{}
					json.Unmarshal(out, &resp)

					Expect(resp.Opts.RelayId).To(Equal(relayId))
				})

				It("stop should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					stopRelayCmd := exec.Command(
						binary,
						"manage",
						"stop",
						"relay",
						"--id", relayId,
					)
					out, err := stopRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Relay stopped"))
				})

				It("resume should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					resumeRelayCmd := exec.Command(
						binary,
						"manage",
						"resume",
						"relay",
						"--id", relayId,
					)
					out, err := resumeRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Relay resumed"))
				})

				It("delete should work", func() {
					if relayId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					deleteRelayCmd := exec.Command(
						binary,
						"manage",
						"delete",
						"relay",
						"--id", relayId,
					)
					out, err := deleteRelayCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Relay deleted"))

					// delete connection
					deleteConnection(binary, connId)
				})

			})

			Describe("tunnel", func() {
				var connId string
				var tunnelId string

				It("create should work", func() {
					connId = createRabbitConnection(binary)

					randID := rand.Int()
					var (
						exchangeName string = fmt.Sprintf("testex-%d", randID)
						queueName    string = fmt.Sprintf("testqueue-%d", randID)
						routingKey   string = fmt.Sprintf("testqueue-%d", randID)
					)
					err := createRabbit(exchangeName, queueName, routingKey)
					Expect(err).ToNot(HaveOccurred())

					type createTunnelResp struct {
						Status struct {
							Message   string `json:"message"`
							RequestId string `json:"requestId"`
						} `json:"status"`
						TunnelId string `json:"tunnelId"`
					}
					batchAPIToken := os.Getenv("TEST_API_TOKEN")
					if batchAPIToken == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no API token")
					}

					createTunnelCmd := exec.Command(
						binary,
						"manage",
						"create",
						"tunnel",
						"rabbit",
						"--x-tunnel-address", "dproxy.dev.batch.sh:443",
						"--connection-id", connId,
						"--tunnel-token", batchAPIToken,
						"--exchange-name", exchangeName,
						"--routing-key", routingKey,
					)
					out, err := createTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					resp := createTunnelResp{}
					json.Unmarshal(out, &resp)
					Expect(string(out)).To(ContainSubstring("Tunnel created"))

					tunnelId = resp.TunnelId
				})

				It("get should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					type getTunnelResp struct {
						Opts struct {
							TunnelId string `json:"tunnelId"`
						}
					}
					getTunnelCmd := exec.Command(
						binary,
						"manage",
						"get",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := getTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					resp := getTunnelResp{}
					json.Unmarshal(out, &resp)

					Expect(resp.Opts.TunnelId).To(Equal(tunnelId))
				})

				It("stop should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					stopTunnelCmd := exec.Command(
						binary,
						"manage",
						"stop",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := stopTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Tunnel replay stopped"))
				})

				It("resume should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					resumeTunnelCmd := exec.Command(
						binary,
						"manage",
						"resume",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := resumeTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Tunnel replay resumed"))
				})

				It("delete should work", func() {
					if tunnelId == "" {
						defer GinkgoRecover()
						Skip("Cannot test, no collection token in creation")
					}

					deleteTunnelCmd := exec.Command(
						binary,
						"manage",
						"delete",
						"tunnel",
						"--id", tunnelId,
					)
					out, err := deleteTunnelCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())
					Expect(string(out)).To(ContainSubstring("Tunnel replay deleted"))

					// delete connection
					deleteConnection(binary, connId)
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
			QueueCapacity:    100,
		}),
	}, nil
}

func newKafkaWriter(address, topic string) (*Kafka, error) {
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
			Addr: skafka.TCP(address),
		},
	}, nil
}

func createKafkaConnection(binary string) string {

	type CreateConnResp struct {
		ConnectionId string `json:"connectionId"`
	}

	connName := fmt.Sprintf("FunctionalTestConnection-%d", rand.Int())
	createConnCmd := exec.Command(
		binary,
		"manage",
		"create",
		"connection",
		"kafka",
		"--name", connName,
		"--address", "localhost:9092",
	)
	out, err := createConnCmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())

	resp := CreateConnResp{}
	json.Unmarshal(out, &resp)
	return resp.ConnectionId
}

func deleteConnection(binary string, connId string) {
	deleteConnCmd := exec.Command(
		binary,
		"manage",
		"delete",
		"connection",
		"--id", connId,
	)
	_, err := deleteConnCmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
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

func createNatsJsStream(streamName string) error {
	nc, _ := nats.Connect("nats://localhost:4222")
	js, _ := nc.JetStream()
	_, err := js.AddStream(&nats.StreamConfig{
		Name: streamName,
		//Subjects: []string{streamSubjects},
	})
	if err != nil {
		return err
	}
	return nil
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

func createRabbitConnection(binary string) string {

	type CreateConnResp struct {
		ConnectionId string `json:"connectionId"`
	}

	connName := fmt.Sprintf("FunctionalTestConnection-%d", rand.Int())
	createConnCmd := exec.Command(
		binary,
		"manage",
		"create",
		"connection",
		"rabbit",
		"--name", connName,
		"--address", "amqp://localhost",
	)
	out, err := createConnCmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())

	resp := CreateConnResp{}
	json.Unmarshal(out, &resp)
	return resp.ConnectionId
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

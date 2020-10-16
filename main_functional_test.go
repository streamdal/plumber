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
	"fmt"
	"math/rand"
	"os/exec"
	"runtime"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	skafka "github.com/segmentio/kafka-go"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	SetDefaultEventuallyTimeout(time.Second * 30)
}

var _ = Describe("Functional", func() {
	var (
		//kafkaAddress         = "localhost:9092"
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

	//Describe("Kafka", func() {
	//	var (
	//		kafka      *Kafka
	//		kafkaTopic = fmt.Sprintf("plumber-test-%d", rand.Int())
	//	)
	//
	//	Describe("write", func() {
	//		BeforeEach(func() {
	//			var err error
	//
	//			kafka, err = newKafka(kafkaAddress, kafkaTopic)
	//
	//			Expect(err).ToNot(HaveOccurred())
	//		})
	//
	//		Context("plain input, plain output", func() {
	//			It("should work", func() {
	//				randString := fmt.Sprintf("kafka-random-%d", rand.Int())
	//
	//				cmd := exec.Command(binary, "write", "kafka", "--address", kafkaAddress,
	//					"--topic", kafkaTopic, "--input-data", randString)
	//
	//				_, err := cmd.CombinedOutput()
	//
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				// Read message from kafka topic, verify it's set to randString
	//				msg, err := kafka.Reader.ReadMessage(context.Background())
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				Expect(err).ToNot(HaveOccurred())
	//				Expect(string(msg.Value)).To(Equal(randString))
	//			})
	//		})
	//
	//		Context("jsonpb input, protobuf output", func() {
	//			It("should work", func() {
	//				// We use "Outbound" here because it's simple
	//				cmd := exec.Command(binary, "write", "kafka", "--address", kafkaAddress,
	//					"--topic", kafkaTopic, "--input-type", "jsonpb", "--output-type", "protobuf",
	//					"--input-file", sampleOutboundJSONPB, "--protobuf-dir", protoSchemasDir,
	//					"--protobuf-root-message", "Outbound")
	//
	//				_, err := cmd.CombinedOutput()
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				// Read message from kafka topic; verify it matches what we wrote
	//				msg, err := kafka.Reader.ReadMessage(context.Background())
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				// Verify we wrote a valid protobuf message
	//				outbound := &events.Outbound{}
	//
	//				err = proto.Unmarshal(msg.Value, outbound)
	//
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				// Verify that the values are the same
	//				jsonData, err := ioutil.ReadFile(sampleOutboundJSONPB)
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				jsonMap := make(map[string]string, 0)
	//
	//				err = json.Unmarshal(jsonData, &jsonMap)
	//				Expect(err).ToNot(HaveOccurred())
	//
	//				Expect(outbound.ReplayId).To(Equal(jsonMap["replay_id"]))
	//
	//				// []byte is encoded as base64, so we have to encode it to
	//				// verify against source JSON
	//				encodedBlob := base64.StdEncoding.EncodeToString(outbound.Blob)
	//
	//				Expect(encodedBlob).To(Equal(jsonMap["blob"]))
	//			})
	//		})
	//	})
	//
	//	Describe("read", func() {
	//		var ()
	//
	//		BeforeEach(func() {
	//		})
	//
	//		Context("plain output", func() {
	//			It("should work", func() {
	//			})
	//		})
	//
	//		Context("protobuf output", func() {
	//			It("should work", func() {
	//			})
	//		})
	//	})
	//})

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

					// First write the message to SQS
					writeCmd := exec.Command(
						binary,
						"write",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--input-data", testMessage,
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to exchange '%s'", exchangeName)
					Expect(writeGot).To(ContainSubstring(writeWant))

					// Now try and read from the SQS queue
					readCmd := exec.Command(
						binary,
						"read",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--queue", queueName,
					)

					readOutput, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

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
						"--output-type", "protobuf",
						"--input-file", sampleOutboundJSONPB,
						"--protobuf-dir", protoSchemasDir,
						"--protobuf-root-message", "Outbound",
					)

					writeOut, err := writeCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

					writeGot := string(writeOut[:])
					writeWant := fmt.Sprintf("Successfully wrote message to exchange '%s'", exchangeName)

					Expect(writeGot).To(ContainSubstring(writeWant))

					readCmd := exec.Command(
						binary,
						"read",
						"rabbit",
						"--exchange", exchangeName,
						"--routing-key", routingKey,
						"--queue", queueName,
						"--output-type", "protobuf",
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

				// First write the message to SQS
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
				Expect(err).ToNot(HaveOccurred())

				writeGot := string(writeOut[:])
				writeWant := fmt.Sprintf("Successfully wrote message to exchange '%s'", exchangeName)
				Expect(writeGot).To(ContainSubstring(writeWant))

				// Now try and read from the SQS queue
				readCmd := exec.Command(
					binary,
					"read",
					"rabbit",
					"--avro-schema", "./test-assets/avro/test.avsc",
					"--exchange", exchangeName,
					"--routing-key", routingKey,
					"--queue", queueName,
				)

				readOutput, err := readCmd.CombinedOutput()
				Expect(err).ToNot(HaveOccurred())

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
					)

					readOutput, err := readCmd.CombinedOutput()
					Expect(err).ToNot(HaveOccurred())

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
						"--output-type", "protobuf",
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
						"--output-type", "protobuf",
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
	cmd = exec.Command("docker", "exec", "rabbitmq", "rabbitmqadmin", "declare", "queue", "name="+queueName)
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

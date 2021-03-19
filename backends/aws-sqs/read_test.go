package awssqs

import (
	"errors"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/aws-sqs/types/typesfakes"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/printer/printerfakes"
)

var _ = Describe("AWS SQS Backend", func() {
	logger := logrus.New()
	logger.Out = ioutil.Discard
	log := logrus.NewEntry(logger)

	Context("validateReadOptions", func() {
		It("returns an error when max messages is out of bounds", func() {
			opts := &cli.Options{AWSSQS: &cli.AWSSQSOptions{
				ReadMaxNumMessages: -1,
			}}

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when wait time is out of bounds", func() {
			opts := &cli.Options{AWSSQS: &cli.AWSSQSOptions{
				ReadMaxNumMessages:  1,
				ReadWaitTimeSeconds: 30,
			}}

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
		})

		It("returns nil on valid config", func() {
			opts := &cli.Options{AWSSQS: &cli.AWSSQSOptions{
				ReadMaxNumMessages:  10,
				ReadWaitTimeSeconds: 0,
			}}

			err := validateReadOptions(opts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Read", func() {
		It("returns an error when unable to receive a message", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.ReceiveMessageStub = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
				return nil, errors.New("test error")
			}

			opts := &cli.Options{
				ReadFollow: false,
				AWSSQS:     &cli.AWSSQSOptions{},
			}

			a := &AWSSQS{
				Options: opts,
				Service: sqsFake,
				Log:     log,
			}

			err := a.Read()

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to receive any message(s) from SQS"))
			Expect(sqsFake.ReceiveMessageCallCount()).To(Equal(1))
		})

		It("reads zero messages", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.ReceiveMessageStub = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
				return &sqs.ReceiveMessageOutput{}, nil
			}

			opts := &cli.Options{
				ReadFollow: false,
				AWSSQS:     &cli.AWSSQSOptions{},
			}

			a := &AWSSQS{
				Options: opts,
				Service: sqsFake,
				Log:     log,
				Printer: printer.New(),
			}

			err := a.Read()

			Expect(err).ToNot(HaveOccurred())
			Expect(sqsFake.ReceiveMessageCallCount()).To(Equal(1))
		})

		It("reads a message", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.ReceiveMessageStub = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{Body: aws.String("Test Message")},
					},
				}, nil
			}

			var readMessage string

			printerFake := &printerfakes.FakeIPrinter{}
			printerFake.PrintStub = func(str string) {
				readMessage = str
			}

			opts := &cli.Options{
				ReadFollow:      false,
				ReadLineNumbers: true,
				AWSSQS:          &cli.AWSSQSOptions{},
			}

			a := &AWSSQS{
				Options: opts,
				Service: sqsFake,
				Log:     log,
				Printer: printerFake,
			}

			err := a.Read()

			Expect(err).ToNot(HaveOccurred())
			Expect(sqsFake.ReceiveMessageCallCount()).To(Equal(1))
			Expect(sqsFake.DeleteMessageCallCount()).To(Equal(0))
			Expect(readMessage).To(Equal("1: Test Message"))
		})

		It("errors on failure to decode", func() {
			// TODO
		})

		It("auto deletes after read", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.ReceiveMessageStub = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{Body: aws.String("Test Message")},
					},
				}, nil
			}

			var readMessage string

			printerFake := &printerfakes.FakeIPrinter{}
			printerFake.PrintStub = func(str string) {
				readMessage = str
			}

			opts := &cli.Options{
				ReadFollow: false,
				AWSSQS: &cli.AWSSQSOptions{
					ReadAutoDelete: true,
				},
			}

			a := &AWSSQS{
				Options: opts,
				Service: sqsFake,
				Log:     log,
				Printer: printerFake,
			}

			err := a.Read()

			Expect(err).ToNot(HaveOccurred())
			Expect(sqsFake.ReceiveMessageCallCount()).To(Equal(1))
			Expect(sqsFake.DeleteMessageCallCount()).To(Equal(1))
			Expect(readMessage).To(Equal("Test Message"))
		})
	})
})

package awssqs

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/backends/aws-sqs/types/typesfakes"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
)

var _ = Describe("AWS SQS Backend", func() {

	Context("validateWriteOptions", func() {
		It("returns an error when delay is out of bounds", func() {
			opts := &options.Options{AWSSQS: &options.AWSSQSOptions{
				WriteDelaySeconds: -1,
			}}

			err := validateWriteOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(ErrInvalidWriteDelaySeconds))
		})

		It("returns nil on valid config", func() {
			opts := &options.Options{AWSSQS: &options.AWSSQSOptions{
				WriteDelaySeconds: 10,
			}}

			err := validateWriteOptions(opts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		It("returns an error on failure to publish message to SQS", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.SendMessageStub = func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				return nil, errors.New("test error")
			}

			a := &AWSSQS{
				Options: &options.Options{AWSSQS: &options.AWSSQSOptions{}},
				service: sqsFake,
			}

			err := a.Write([]byte(`test`))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrUnableToSend))
			Expect(sqsFake.SendMessageCallCount()).To(Equal(1))
		})

		It("returns an error on failure to publish message to SQS", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.SendMessageStub = func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				return &sqs.SendMessageOutput{}, nil
			}

			opts := &options.Options{AWSSQS: &options.AWSSQSOptions{
				WriteAttributes: map[string]string{
					"faz": "baz",
				},
			}}

			a := &AWSSQS{
				Options: opts,
				service: sqsFake,
				printer: printer.New(),
			}

			err := a.Write([]byte(`test`))

			Expect(err).ToNot(HaveOccurred())
			Expect(sqsFake.SendMessageCallCount()).To(Equal(1))
		})
	})
})

package awssqs

import (
	"context"
	"errors"

	ptypes "github.com/batchcorp/plumber/types"

	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/backends/aws-sqs/types/typesfakes"
	"github.com/batchcorp/plumber/options"
)

var _ = Describe("AWS SQS Backend", func() {

	Context("validateWriteOptions", func() {
		It("returns an error when delay is out of bounds", func() {
			opts := &options.Options{
				AWSSQS: &options.AWSSQSOptions{
					WriteDelaySeconds: -1,
				},
				Write: &options.WriteOptions{
					InputData: []string{"test"},
				},
			}

			err := validateWriteOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(ErrInvalidWriteDelaySeconds))
		})

		It("returns nil on valid config", func() {
			opts := &options.Options{
				AWSSQS: &options.AWSSQSOptions{
					WriteDelaySeconds: 10,
				},
				Write: &options.WriteOptions{
					InputData: []string{"test"},
				},
			}

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
				Options: &options.Options{
					AWSSQS: &options.AWSSQSOptions{},
					Write: &options.WriteOptions{
						InputData: []string{"test"},
					},
				},
				service: sqsFake,
			}

			errorCh := make(chan *ptypes.ErrorMessage)

			a.Write(context.Background(), errorCh, &ptypes.WriteMessage{Value: []byte(`test`)})

			Expect(errorCh).Should(Receive(&ptypes.ErrorMessage{Error: errors.New(ErrUnableToSend)}))
			Expect(sqsFake.SendMessageCallCount()).To(Equal(1))
		})

		It("returns an error on failure to publish message to SQS", func() {
			sqsFake := &typesfakes.FakeISQSAPI{}
			sqsFake.SendMessageStub = func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				return &sqs.SendMessageOutput{}, nil
			}

			opts := &options.Options{
				AWSSQS: &options.AWSSQSOptions{
					WriteAttributes: map[string]string{
						"faz": "baz",
					},
				},
				Write: &options.WriteOptions{
					InputData: []string{"test"},
				},
			}

			a := &AWSSQS{
				Options: opts,
				service: sqsFake,
			}

			errorCh := make(chan *ptypes.ErrorMessage)

			err := a.Write(context.Background(), errorCh, &ptypes.WriteMessage{Value: []byte(`test`)})

			Expect(err).ToNot(HaveOccurred())
			Expect(sqsFake.SendMessageCallCount()).To(Equal(1))
		})
	})
})

package awssqs

import (
	"context"
	"errors"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/awssqs/sqsfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("AWSSQS Backend", func() {
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		writeOpts = &opts.WriteOptions{
			Awssqs: &opts.WriteGroupAWSSQSOptions{
				Args: &args.AWSSQSWriteArgs{
					QueueName:              "testing.fifo",
					MessageDeduplicationId: "test",
					RemoteAccountId:        "test",
					Attributes: map[string]string{
						"test": "test",
					},
				},
			},
		}
	})

	Context("validateWriteOptions", func() {
		It("validates nil dynamic options", func() {
			err := validateWriteOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyWriteOpts))
		})
		It("validates nil backend group", func() {
			writeOpts.Awssqs = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			writeOpts.Awssqs.Args = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			writeOpts.Awssqs.Args.QueueName = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingQueue))
		})
		It("passes validation", func() {
			err := validateWriteOptions(writeOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		It("validates write options", func() {
			a := &AWSSQS{}
			err := a.Write(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyWriteOpts.Error()))
		})

		It("returns an error when getQueueURL errors", func() {
			fakeSQS := &sqsfakes.FakeSQSAPI{}
			fakeSQS.GetQueueUrlStub = func(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
				return nil, errors.New("test err")
			}

			a := &AWSSQS{
				client: fakeSQS,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}
			err := a.Write(context.Background(), writeOpts, nil, &records.WriteRecord{Input: `test`})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("test err"))
		})

		It("writes to error channel when publish fails", func() {
			fakeSQS := &sqsfakes.FakeSQSAPI{}
			fakeSQS.GetQueueUrlStub = func(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{}, nil
			}

			fakeSQS.SendMessageStub = func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				return nil, errors.New("test err")
			}

			a := &AWSSQS{
				client: fakeSQS,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			// For coverage reasons
			writeOpts.Awssqs.Args.Attributes = nil

			errorsCh := make(chan *records.ErrorRecord, 1)

			err := a.Write(context.Background(), writeOpts, errorsCh, &records.WriteRecord{Input: `test`})

			Expect(err).ToNot(HaveOccurred())
			Expect(errorsCh).Should(Receive())
		})

		It("writes to the queue", func() {
			fakeSQS := &sqsfakes.FakeSQSAPI{}
			fakeSQS.GetQueueUrlStub = func(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{}, nil
			}

			a := &AWSSQS{
				client: fakeSQS,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := a.Write(context.Background(), writeOpts, nil, &records.WriteRecord{Input: `test`})

			Expect(err).ToNot(HaveOccurred())
		})
	})
})

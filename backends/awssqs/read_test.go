package awssqs

import (
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
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
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		readOpts = &opts.ReadOptions{
			Awssqs: &opts.ReadGroupAWSSQSOptions{
				Args: &args.AWSSQSReadArgs{
					QueueName:               "test",
					RemoteAccountId:         "test",
					MaxNumMessages:          1,
					ReceiveRequestAttemptId: "",
					AutoDelete:              true,
					WaitTimeSeconds:         1,
				},
			},
		}
	})

	Context("validateReadOptions", func() {
		It("validates nil read options", func() {
			err := validateReadOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingReadOptions))
		})
		It("validates missing backend group", func() {
			readOpts.Awssqs = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.Awssqs.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			readOpts.Awssqs.Args.QueueName = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingQueue))
		})

		It("validates wait time seconds", func() {
			readOpts.Awssqs.Args.WaitTimeSeconds = -1
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidWaitTime))
		})

		It("validates max num messages", func() {
			readOpts.Awssqs.Args.MaxNumMessages = 0
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidMaxNumMessages))
		})
	})

	Context("convertPointerMap", func() {
		It("converts map[string]*string to map[string]string", func() {
			m := map[string]*string{
				"a": aws.String("b"),
				"c": aws.String("d"),
			}
			m2 := convertPointerMap(m)
			Expect(m2).To(Equal(map[string]string{
				"a": "b",
				"c": "d",
			}))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			err := (&AWSSQS{}).Read(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid read options"))
		})
		It("reads from SQS", func() {
			errorsCh := make(chan *records.ErrorRecord, 1)
			resultsCh := make(chan *records.ReadRecord, 1)

			fakeSQS := &sqsfakes.FakeSQSAPI{}
			fakeSQS.GetQueueUrlStub = func(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{}, nil
			}

			fakeSQS.ReceiveMessageStub = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{
							MessageId: aws.String("test"),
							Body:      aws.String("test"),
						},
					},
				}, nil
			}

			a := &AWSSQS{
				client: fakeSQS,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := a.Read(context.Background(), readOpts, resultsCh, errorsCh)

			Expect(err).ToNot(HaveOccurred())
			Expect(fakeSQS.ReceiveMessageCallCount()).To(Equal(1))
			Expect(resultsCh).Should(Receive())
		})
	})
})

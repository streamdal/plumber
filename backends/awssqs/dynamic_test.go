package awssqs

import (
	"context"
	"errors"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/backends/awssqs/sqsfakes"
	"github.com/batchcorp/plumber/dynamic/dynamicfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("AWSSQS Backend", func() {
	var dynamicOpts *opts.DynamicOptions

	BeforeEach(func() {
		dynamicOpts = &opts.DynamicOptions{
			AwsSqs: &opts.DynamicGroupAWSSQSOptions{
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

	Context("validateDynamicOptions", func() {
		It("validates nil dynamic options", func() {
			err := validateDynamicOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			dynamicOpts.AwsSqs = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.AwsSqs.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			dynamicOpts.AwsSqs.Args.QueueName = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingQueue))
		})
		It("passes validation", func() {
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Dynamic", func() {
		var fakeDynamic *dynamicfakes.FakeIDynamic

		BeforeEach(func() {
			fakeDynamic = &dynamicfakes.FakeIDynamic{}
			fakeDynamic.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}
		})

		It("validates dynamic options", func() {
			err := (&AWSSQS{}).Dynamic(context.Background(), nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
		})

		It("returns an error on failure to write a message", func() {
			fakeSQS := &sqsfakes.FakeSQSAPI{}
			fakeSQS.GetQueueUrlStub = func(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{}, nil
			}
			fakeSQS.SendMessageStub = func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				return nil, errors.New("test err")
			}

			p := &AWSSQS{
				client: fakeSQS,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Dynamic(context.Background(), dynamicOpts, fakeDynamic)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to replay message"))
			Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
			Expect(fakeSQS.SendMessageCallCount()).To(Equal(1))
		})

		It("replays a message", func() {
			ctx, cancel := context.WithCancel(context.Background())

			fakeSQS := &sqsfakes.FakeSQSAPI{}
			fakeSQS.GetQueueUrlStub = func(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{}, nil
			}
			fakeSQS.SendMessageStub = func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				defer cancel()
				return &sqs.SendMessageOutput{}, nil
			}

			p := &AWSSQS{
				client: fakeSQS,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Dynamic(ctx, dynamicOpts, fakeDynamic)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
			Expect(fakeSQS.SendMessageCallCount()).To(Equal(1))
		})
	})

})

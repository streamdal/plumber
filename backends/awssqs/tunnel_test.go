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
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/awssqs/sqsfakes"
	"github.com/batchcorp/plumber/tunnel/tunnelfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("AWSSQS Backend", func() {
	var tunnelOpts *opts.DynamicOptions

	BeforeEach(func() {
		tunnelOpts = &opts.DynamicOptions{
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

	Context("validateTunnelOptions", func() {
		It("validates nil tunnel options", func() {
			err := validateTunnelOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.AwsSqs = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.AwsSqs.Args = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			tunnelOpts.AwsSqs.Args.QueueName = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingQueue))
		})
		It("passes validation", func() {
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Tunnel", func() {
		var fakeDynamic *tunnelfakes.FakeIDynamic

		BeforeEach(func() {
			fakeDynamic = &tunnelfakes.FakeIDynamic{}
			fakeDynamic.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}
		})

		It("validates tunnel options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := (&AWSSQS{}).Tunnel(context.Background(), nil, nil, errorCh)
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

			errorCh := make(chan *records.ErrorRecord)
			err := p.Tunnel(context.Background(), tunnelOpts, fakeDynamic, errorCh)
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

			errorCh := make(chan *records.ErrorRecord)
			err := p.Tunnel(ctx, tunnelOpts, fakeDynamic, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
			Expect(fakeSQS.SendMessageCallCount()).To(Equal(1))
		})
	})

})

package awssns

import (
	"context"
	"errors"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/awssns/snsfakes"
	"github.com/batchcorp/plumber/tunnel/tunnelfakes"
	"github.com/batchcorp/plumber/validate"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("", func() {
	defer GinkgoRecover()

	Context("validateTunnelOptions", func() {
		It("validtes nil tunnel options", func() {
			err := validateTunnelOptions(nil)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyTunnelOpts))
		})

		It("validates nil backend group", func() {
			tunnelOpts := &opts.TunnelOptions{}

			err := validateTunnelOptions(tunnelOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})

		It("validates nil backend args", func() {
			tunnelOpts := &opts.TunnelOptions{
				AwsSns: &opts.TunnelGroupAWSSNSOptions{},
			}

			err := validateTunnelOptions(tunnelOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})

		It("validates empty topic", func() {
			tunnelOpts := &opts.TunnelOptions{
				AwsSns: &opts.TunnelGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "",
					},
				},
			}

			err := validateTunnelOptions(tunnelOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingTopicARN))
		})

		It("validates topic value", func() {
			tunnelOpts := &opts.TunnelOptions{
				AwsSns: &opts.TunnelGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "invalidtopic",
					},
				},
			}

			err := validateTunnelOptions(tunnelOpts)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("'invalidtopic' is not a valid ARN"))
		})

		It("passes validation", func() {
			tunnelOpts := &opts.TunnelOptions{
				AwsSns: &opts.TunnelGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "arn:aws:sns:us-east-1:123456789012:topic",
					},
				},
			}

			err := validateTunnelOptions(tunnelOpts)

			Expect(err).To(BeNil())
		})
	})

	Context("Tunnel", func() {
		var fakeSNS *snsfakes.FakeSNSAPI
		var fakeTunnel *tunnelfakes.FakeITunnel
		var errorCh chan *records.ErrorRecord
		tunnelOpts := &opts.TunnelOptions{
			AwsSns: &opts.TunnelGroupAWSSNSOptions{
				Args: &args.AWSSNSWriteArgs{
					Topic: "arn:aws:sns:us-east-1:123456789012:topic",
				},
			},
		}

		BeforeEach(func() {
			errorCh = make(chan *records.ErrorRecord)

			fakeSNS = &snsfakes.FakeSNSAPI{}

			fakeTunnel = &tunnelfakes.FakeITunnel{}
			fakeTunnel.StartStub = func(context.Context, string, chan<- *records.ErrorRecord) error {
				return nil
			}
			fakeTunnel.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}

		})

		It("validates tunnel options", func() {
			err := (&AWSSNS{}).Tunnel(context.Background(), nil, nil, nil)

			Expect(err.Error()).To(ContainSubstring("unable to validate tunnel options"))
			Expect(fakeTunnel.StartCallCount()).To(Equal(0))
			Expect(fakeTunnel.ReadCallCount()).To(Equal(0))
		})

		It("validates creating of tunnel", func() {
			fakeTunnel.StartStub = func(context.Context, string, chan<- *records.ErrorRecord) error {
				return errors.New("start error")
			}

			p := &AWSSNS{
				log: logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Tunnel(context.Background(), tunnelOpts, fakeTunnel, errorCh)
			Expect(err.Error()).To(ContainSubstring("unable to create tunnel"))
			Expect(fakeTunnel.StartCallCount()).To(Equal(1))
			Expect(fakeTunnel.ReadCallCount()).To(Equal(0))

		})

		It("returns an error on failure to write a message", func() {
			ctx, cancel := context.WithCancel(context.Background())

			fakeSNS.PublishStub = func(pi *sns.PublishInput) (*sns.PublishOutput, error) {
				defer cancel()
				return &sns.PublishOutput{}, errors.New("publish error")
			}

			p := &AWSSNS{
				Service: fakeSNS,
				log:     logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Tunnel(ctx, tunnelOpts, fakeTunnel, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeTunnel.StartCallCount()).To(Equal(1))
			Expect(fakeTunnel.ReadCallCount()).To(Equal(1))
			Expect(fakeSNS.PublishCallCount()).To(Equal(1))

		})

		It("replays a message", func() {
			ctx, cancel := context.WithCancel(context.Background())

			fakeSNS.PublishStub = func(pi *sns.PublishInput) (*sns.PublishOutput, error) {
				defer cancel()
				return &sns.PublishOutput{}, nil
			}

			p := &AWSSNS{
				Service: fakeSNS,
				log:     logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Tunnel(ctx, tunnelOpts, fakeTunnel, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeTunnel.StartCallCount()).To(Equal(1))
			Expect(fakeTunnel.ReadCallCount()).To(Equal(1))
			Expect(fakeSNS.PublishCallCount()).To(Equal(1))
		})
	})
})

package awskinesis

import (
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/kinesis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/awskinesis/kinesisfakes"
	"github.com/batchcorp/plumber/tunnel/tunnelfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("AWS Kinesis Backend", func() {
	var tunnelOpts *opts.TunnelOptions

	BeforeEach(func() {
		tunnelOpts = &opts.TunnelOptions{
			AwsKinesis: &opts.TunnelGroupAWSKinesisOptions{
				Args: &args.AWSKinesisWriteArgs{
					Stream:         "test",
					PartitionKey:   "test",
					SequenceNumber: "1",
				},
			},
		}
	})

	Context("validateTunnelOptions", func() {
		It("validates nil tunnel options", func() {
			err := validateTunnelOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyTunnelOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.AwsKinesis = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.AwsKinesis.Args = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			tunnelOpts.AwsKinesis.Args.Stream = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyStream))
		})
		It("validates empty partition key", func() {
			tunnelOpts.AwsKinesis.Args.PartitionKey = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyPartitionKey))
		})
		It("passes validation", func() {
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Tunnel", func() {
		var fakeTunnel *tunnelfakes.FakeITunnel

		BeforeEach(func() {
			fakeTunnel = &tunnelfakes.FakeITunnel{}
			fakeTunnel.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}
		})

		It("validates tunnel options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := (&Kinesis{}).Tunnel(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyTunnelOpts.Error()))
		})

		It("replays a message", func() {
			ctx, cancel := context.WithCancel(context.Background())

			fakeKinesis := &kinesisfakes.FakeKinesisAPI{}
			fakeKinesis.PutRecordStub = func(*kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
				defer cancel()
				return &kinesis.PutRecordOutput{}, nil
			}

			p := &Kinesis{
				client: fakeKinesis,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errorCh := make(chan *records.ErrorRecord)
			err := p.Tunnel(ctx, tunnelOpts, fakeTunnel, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeTunnel.ReadCallCount()).To(Equal(1))
			Expect(fakeKinesis.PutRecordCallCount()).To(Equal(1))
		})
	})
})

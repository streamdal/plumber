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
	"github.com/batchcorp/plumber/dynamic/dynamicfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("AWS Kinesis Backend", func() {
	var dynamicOpts *opts.DynamicOptions

	BeforeEach(func() {
		dynamicOpts = &opts.DynamicOptions{
			AwsKinesis: &opts.DynamicGroupAWSKinesisOptions{
				Args: &args.AWSKinesisWriteArgs{
					Stream:         "test",
					PartitionKey:   "test",
					SequenceNumber: "1",
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
			dynamicOpts.AwsKinesis = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.AwsKinesis.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			dynamicOpts.AwsKinesis.Args.Stream = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyStream))
		})
		It("validates empty partition key", func() {
			dynamicOpts.AwsKinesis.Args.PartitionKey = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyPartitionKey))
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
			errorCh := make(chan *records.ErrorRecord)
			err := (&Kinesis{}).Dynamic(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
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
			err := p.Dynamic(ctx, dynamicOpts, fakeDynamic, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
			Expect(fakeKinesis.PutRecordCallCount()).To(Equal(1))
		})
	})
})

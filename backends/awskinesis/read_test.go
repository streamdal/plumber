package awskinesis

import (
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/awskinesis/kinesisfakes"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("AWS Kinesis Backend", func() {
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		readOpts = &opts.ReadOptions{
			AwsKinesis: &opts.ReadGroupAWSKinesisOptions{
				Args: &args.AWSKinesisReadArgs{
					Stream:     "test",
					Shard:      "ShardId-0000000001",
					MaxRecords: 1,
					ReadLatest: true,
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
			readOpts.AwsKinesis = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.AwsKinesis.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty stream", func() {
			readOpts.AwsKinesis.Args.Stream = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyStream))
		})

		It("returns an error when reading from all shards, but with a sequence number specified", func() {
			readOpts.AwsKinesis.Args.Shard = ""
			readOpts.AwsKinesis.Args.ReadSequenceNumber = "123"
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyShardWithSequence))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			err := (&Kinesis{}).Read(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid read options"))
		})
		It("reads from Kinesis", func() {
			errorsCh := make(chan *records.ErrorRecord, 1)
			resultsCh := make(chan *records.ReadRecord, 1)

			fakeKinesis := &kinesisfakes.FakeKinesisAPI{}
			fakeKinesis.GetRecordsWithContextStub = func(context.Context, *kinesis.GetRecordsInput, ...request.Option) (*kinesis.GetRecordsOutput, error) {
				return &kinesis.GetRecordsOutput{
					Records: []*kinesis.Record{
						{
							Data: []byte("test"),
						},
					},
				}, nil
			}
			fakeKinesis.GetShardIteratorStub = func(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
				return &kinesis.GetShardIteratorOutput{
					ShardIterator: aws.String("test"),
				}, nil
			}

			a := &Kinesis{
				client: fakeKinesis,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := a.Read(context.Background(), readOpts, resultsCh, errorsCh)

			Expect(err).ToNot(HaveOccurred())
			Expect(fakeKinesis.GetRecordsWithContextCallCount()).To(Equal(1))
			Expect(fakeKinesis.GetShardIteratorCallCount()).To(Equal(1))
			Expect(resultsCh).Should(Receive())
		})
	})
})

package awskinesis

import (
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/kinesis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/awskinesis/kinesisfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("AWS Kinesis Backend", func() {
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		writeOpts = &opts.WriteOptions{
			AwsKinesis: &opts.WriteGroupAWSKinesisOptions{
				Args: &args.AWSKinesisWriteArgs{
					Stream:         "test",
					PartitionKey:   "test",
					SequenceNumber: "1",
				},
			},
		}
	})

	Context("validateWriteOptions", func() {
		It("validates nil write options", func() {
			err := validateWriteOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyWriteOpts))
		})
		It("validates nil backend group", func() {
			writeOpts.AwsKinesis = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			writeOpts.AwsKinesis.Args = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty stream", func() {
			writeOpts.AwsKinesis.Args.Stream = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyStream))
		})
		It("validates empty partition key", func() {
			writeOpts.AwsKinesis.Args.PartitionKey = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyPartitionKey))
		})
		It("passes validation", func() {
			err := validateWriteOptions(writeOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		It("validates write options", func() {
			err := (&Kinesis{}).Write(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyWriteOpts.Error()))
		})

		It("should write a message to Kinesis", func() {
			fakeKinesis := &kinesisfakes.FakeKinesisAPI{}
			fakeKinesis.PutRecordStub = func(*kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
				return &kinesis.PutRecordOutput{}, nil
			}

			a := &Kinesis{
				client: fakeKinesis,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errorCh := make(chan *records.ErrorRecord, 1)

			err := a.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: "test"})
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeKinesis.PutRecordCallCount()).To(Equal(1))
			Expect(errorCh).ToNot(Receive())
		})
	})
})

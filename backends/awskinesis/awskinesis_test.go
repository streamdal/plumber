package awskinesis

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

var _ = Describe("AWS Kinesis Backend", func() {
	var connOpts *opts.ConnectionOptions

	BeforeEach(func() {
		connOpts = &opts.ConnectionOptions{
			Conn: &opts.ConnectionOptions_AwsKinesis{
				AwsKinesis: &args.AWSKinesisConn{
					AwsRegion:          "us-east-1",
					AwsSecretAccessKey: "test",
					AwsAccessKeyId:     "test",
				},
			},
		}
	})

	Context("validateBaseConnOpts", func() {
		It("validates conn presence", func() {
			err := validateBaseConnOpts(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnOpts))
		})
		It("validates conn config", func() {
			connOpts = &opts.ConnectionOptions{}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnCfg))
		})
		It("validates Kinesis options presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsKinesis{
					AwsKinesis: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})
		It("validates AWS secret access key", func() {
			connOpts.GetAwsKinesis().AwsSecretAccessKey = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSSecretAccessKey))
		})
		It("validates AWS region", func() {
			connOpts.GetAwsKinesis().AwsRegion = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSRegion))
		})
		It("validates AWS key ID", func() {
			connOpts.GetAwsKinesis().AwsAccessKeyId = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSAccessKeyID))
		})
	})

	Context("Name", func() {
		It("returns backend name", func() {
			Expect((&Kinesis{}).Name()).To(Equal(BackendName))
		})
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			Expect((&Kinesis{}).Test(nil)).To(Equal(types.NotImplementedErr))
		})
	})

	Context("Close", func() {
		It("returns nil", func() {
			Expect((&Kinesis{}).Close(nil)).To(BeNil())
		})
	})
})

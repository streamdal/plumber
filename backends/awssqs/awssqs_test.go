package awssqs

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/validate"
)

var _ = Describe("AWSSQS Backend", func() {
	var connOpts *opts.ConnectionOptions

	BeforeEach(func() {
		connOpts = &opts.ConnectionOptions{
			Conn: &opts.ConnectionOptions_AwsSqs{
				AwsSqs: &args.AWSSQSConn{
					AwsRegion:          "us-east-1",
					AwsSecretAccessKey: "test",
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
		It("validates AWSSQS presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSqs{
					AwsSqs: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})
		It("validates AWS secret access key", func() {
			connOpts.GetAwsSqs().AwsSecretAccessKey = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSSecretAccessKey))
		})
		It("validates AWS region", func() {
			connOpts.GetAwsSqs().AwsRegion = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSRegion))
		})
	})

	Context("Name", func() {
		It("returns backend name", func() {
			Expect((&AWSSQS{}).Name()).To(Equal("aws-sqs"))
		})
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			Expect((&AWSSQS{}).Test(nil)).To(MatchError("not implemented"))
		})
	})

	Context("Close", func() {
		It("returns nil", func() {
			Expect((&AWSSQS{}).Close(nil)).To(BeNil())
		})
	})
})

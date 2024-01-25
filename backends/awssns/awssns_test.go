package awssns

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/validate"
)

var _ = Describe("AWSSNS Backend", func() {
	defer GinkgoRecover()

	Context("validateBaseConnOpts", func() {
		It("validates conn presence", func() {
			err := validateBaseConnOpts(nil)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnOpts))
		})

		It("validates conn config", func() {
			connOpts := &opts.ConnectionOptions{}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnCfg))
		})

		It("validates AWSSNS presence", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSns{
					AwsSns: nil,
				},
			}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})

		It("validates AWS profile", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSns{
					AwsSns: &args.AWSSNSConn{
						AwsProfile: "profile",
					},
				},
			}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(BeNil())
		})

		It("validates AWS secret access key", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSns{
					AwsSns: &args.AWSSNSConn{
						AwsSecretAccessKey: "",
					},
				},
			}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSSecretAccessKey))
		})

		It("validates AWS region", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSns{
					AwsSns: &args.AWSSNSConn{
						AwsSecretAccessKey: "secret",
						AwsRegion:          "",
					},
				},
			}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSRegion))
		})

		It("validates AWS access key ID", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSns{
					AwsSns: &args.AWSSNSConn{
						AwsSecretAccessKey: "secret",
						AwsRegion:          "region",
						AwsAccessKeyId:     "",
					},
				},
			}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAWSAccessKeyID))
		})

		It("passes validation", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_AwsSns{
					AwsSns: &args.AWSSNSConn{
						AwsSecretAccessKey: "secret",
						AwsRegion:          "region",
						AwsAccessKeyId:     "key",
					},
				},
			}

			err := validateBaseConnOpts(connOpts)

			Expect(err).To(BeNil())
		})
	})

	Context("Name", func() {
		It("returns backend name", func() {
			Expect((&AWSSNS{}).Name()).To(Equal("AWSSNS"))
		})
	})

	Context("Close", func() {
		It("returns nil", func() {
			Expect((&AWSSNS{}).Close(context.Background())).To(BeNil())
		})
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			Expect((&AWSSNS{}).Test(context.Background())).To(MatchError("not implemented"))
		})
	})

	Context("New", func() {
		It("validates connection options", func() {
			_, err := New(nil)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to validate options"))
		})
	})
})

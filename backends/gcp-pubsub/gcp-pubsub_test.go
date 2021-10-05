package gcppubsub

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/cli"
)

var _ = Describe("GCP PubSub Backend", func() {
	Context("validateOpts", func() {
		It("returns error when no options passed", func() {
			err := validateOpts(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingOptions))
		})

		It("returns error when n GCP options passed", func() {
			opts := &cli.Options{}

			err := validateOpts(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingGCPOptions))
		})

		It("returns error when no credential option is provided", func() {
			opts := &cli.Options{GCPPubSub: &cli.GCPPubSubOptions{}}

			err := validateOpts(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingCredentials))
		})

		It("returns nil on valid optionns", func() {
			opts := &cli.Options{
				GCPPubSub: &cli.GCPPubSubOptions{
					CredentialsJSON: `{"foo": "value"}`,
				},
			}

			err := validateOpts(opts)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})

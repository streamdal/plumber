package gcppubsub

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber/options"
)

var _ = Describe("GCP PubSub Backend", func() {
	Context("validateOpts", func() {
		It("returns error when no options passed", func() {
			err := validateOpts(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingOptions))
		})

		It("returns error when n GCP options passed", func() {
			opts := &options.Options{}

			err := validateOpts(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingGCPOptions))
		})

		It("returns error when no credential option is provided", func() {
			opts := &options.Options{GCPPubSub: &options.GCPPubSubOptions{}}

			err := validateOpts(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingCredentials))
		})

		It("returns nil on valid optionns", func() {
			opts := &options.Options{
				GCPPubSub: &options.GCPPubSubOptions{
					CredentialsJSON: `{"foo": "value"}`,
				},
			}

			err := validateOpts(opts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			g := &GCPPubSub{}

			err := g.Test(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(types.NotImplementedErr))
		})
	})

	Context("Lag", func() {
		It("returns unsupported error", func() {
			g := &GCPPubSub{}

			err := g.Lag(context.Background(), nil, 0)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(types.UnsupportedFeatureErr))
		})
	})
})

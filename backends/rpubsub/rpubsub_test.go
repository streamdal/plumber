package rpubsub

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

var _ = Describe("Redis PubSub Backend", func() {
	Context("Name", func() {
		It("should return the name", func() {
			Expect((&RedisPubsub{}).Name()).To(Equal(BackendName))
		})
	})

	Context("Close", func() {
		// Unable to test as Close() is not present in the interface
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			Expect((&RedisPubsub{}).Test(nil)).To(Equal(types.NotImplementedErr))
		})
	})

	Context("validateBaseConnOpts", func() {
		var connOpts *opts.ConnectionOptions

		BeforeEach(func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_RedisPubsub{
					RedisPubsub: &args.RedisPubSubConn{
						Username: "test",
						Password: "test",
					},
				},
			}
		})

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
		It("validates RedisPubsub presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_RedisPubsub{
					RedisPubsub: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})

		It("validates username and password", func() {
			// Password can be specified without username,
			// but password must be specified if username is specified
			connOpts.GetRedisPubsub().Password = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingPassword))
		})
	})
})

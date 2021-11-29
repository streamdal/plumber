package rstreams

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber/types"
)

var _ = Describe("Redis Streams Backend", func() {
	Context("Name", func() {
		It("Returns name", func() {
			Expect((&RedisStreams{}).Name()).To(Equal(BackendName))
		})
	})

	Context("Close", func() {
		// Can't test due to lack of interface
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			Expect((&RedisStreams{}).Test(nil)).To(Equal(types.NotImplementedErr))
		})
	})
	Context("Close", func() {
		// Unable to test as Close() is not present in the interface
	})

	Context("validateBaseConnOpts", func() {
		var connOpts *opts.ConnectionOptions

		BeforeEach(func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_RedisStreams{
					RedisStreams: &args.RedisStreamsConn{
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
		It("validates RedisStreams presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_RedisStreams{
					RedisStreams: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})

		It("validates username and password", func() {
			// Password can be specified without username,
			// but password must be specified if username is specified
			connOpts.GetRedisStreams().Password = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingPassword))
		})
	})
})

package rabbit_streams

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("Rabbit Streams Backend", func() {
	Context("Name", func() {
		It("should return the name", func() {
			Expect((&RabbitStreams{}).Name()).To(Equal(BackendName))
		})
	})

	Context("Close", func() {
		// Unable to test as Close() is not present in the interface
	})

	Context("Test", func() {
		It("returns not implemented error", func() {
			Expect((&RabbitStreams{}).Test(nil)).To(Equal(types.NotImplementedErr))
		})
	})

	Context("validateBaseConnOpts", func() {
		var connOpts *opts.ConnectionOptions

		BeforeEach(func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_RabbitStreams{
					RabbitStreams: &args.RabbitStreamsConn{
						Dsn:      "localhost",
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
		It("validates RabbitStreams presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_RabbitStreams{
					RabbitStreams: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})

		It("validates empty DSN", func() {
			connOpts.GetRabbitStreams().Dsn = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyDSN))
		})
	})
})

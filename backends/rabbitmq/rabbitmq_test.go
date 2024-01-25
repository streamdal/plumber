package rabbitmq

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/streamdal/plumber/types"

	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/backends/rabbitmq/rabbitfakes"
)

var _ = Describe("RabbitMQ Backend", func() {
	var connOpts *opts.ConnectionOptions

	BeforeEach(func() {
		connOpts = &opts.ConnectionOptions{
			Conn: &opts.ConnectionOptions_Rabbit{
				Rabbit: &args.RabbitConn{
					Address:       "amqp://localhost:5672",
					UseTls:        false,
					TlsSkipVerify: false,
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
		It("validates MQTT presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Rabbit{
					Rabbit: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})
		It("validates address", func() {
			connOpts.GetRabbit().Address = ""
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingAddress))
		})
	})

	Context("Name", func() {
		It("returns backend name", func() {
			Expect((&RabbitMQ{}).Name()).To(Equal("rabbitmq"))
		})
	})

	Context("Close", func() {
		It("returns nil if no client set", func() {
			Expect((&RabbitMQ{}).Close(nil)).To(BeNil())
		})
		It("closes rabbit connection", func() {
			fakeRabbit := &rabbitfakes.FakeIRabbit{}
			r := &RabbitMQ{client: fakeRabbit}

			Expect(r.Close(nil)).To(BeNil())
			Expect(fakeRabbit.CloseCallCount()).To(Equal(1))
		})
	})

	Context("Test", func() {
		It("returns not implemented", func() {
			Expect((&RabbitMQ{}).Test(nil)).To(Equal(types.NotImplementedErr))
		})
	})

	Context("New", func() {
		It("validates base connection options", func() {
			_, err := New(nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid connection options"))
		})
		It("returns an instance of RabbitMQ struct", func() {
			r, err := New(connOpts)
			Expect(err).ToNot(HaveOccurred())
			Expect(r).To(BeAssignableToTypeOf(&RabbitMQ{}))
		})
	})
})

package rabbitmq

import (
	"context"
	"io/ioutil"

	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/rabbitmq/rabbitfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("RabbitMQ Backend", func() {
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		writeOpts = &opts.WriteOptions{
			Rabbit: &opts.WriteGroupRabbitOptions{
				Args: &args.RabbitWriteArgs{
					RoutingKey:   "testing",
					ExchangeName: "testing",
				},
			},
		}
	})

	Context("validateDynamicOptions", func() {
		It("validates nil dynamic options", func() {
			err := validateWriteOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyWriteOpts))
		})
		It("validates nil backend group", func() {
			writeOpts.Rabbit = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			writeOpts.Rabbit.Args = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty routing key", func() {
			writeOpts.Rabbit.Args.RoutingKey = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyRoutingKey))
		})
		It("validates empty exchange name", func() {
			writeOpts.Rabbit.Args.ExchangeName = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyExchangeName))
		})
		It("passes validation", func() {
			err := validateWriteOptions(writeOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		It("validates write options", func() {
			r := &RabbitMQ{}
			err := r.Write(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyWriteOpts.Error()))
		})

		It("error channel receives a message when publish fails", func() {
			testErr := errors.New("test err")

			fakeRabbit := &rabbitfakes.FakeIRabbit{}
			fakeRabbit.PublishStub = func(context.Context, string, []byte) error {
				return testErr
			}

			errorCh := make(chan *records.ErrorRecord, 1)

			r := &RabbitMQ{client: fakeRabbit}
			err := r.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: `test`})

			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).Should(Receive())
		})

		It("publishes successfully", func() {
			errorCh := make(chan *records.ErrorRecord, 1)

			fakeRabbit := &rabbitfakes.FakeIRabbit{}

			r := &RabbitMQ{
				client: fakeRabbit,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}
			err := r.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: `test`})

			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).ShouldNot(Receive())
			Expect(fakeRabbit.PublishCallCount()).To(Equal(1))
		})
	})
})

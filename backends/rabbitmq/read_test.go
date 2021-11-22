package rabbitmq

import (
	"context"
	"io/ioutil"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/rabbitmq/rabbitfakes"
	"github.com/batchcorp/plumber/validate"
	"github.com/batchcorp/rabbit"
)

var _ = Describe("RabbitMQ Backend", func() {
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		readOpts = &opts.ReadOptions{
			Rabbit: &opts.ReadGroupRabbitOptions{
				Args: &args.RabbitReadArgs{
					ExchangeName:   "test",
					QueueName:      "test",
					BindingKey:     "test",
					QueueExclusive: false,
					QueueDeclare:   false,
					QueueDurable:   false,
					AutoAck:        false,
					ConsumerTag:    "test",
					QueueDelete:    false,
				},
			},
		}
	})

	Context("validateReadOptions", func() {
		It("validates nil read options", func() {
			err := validateReadOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingReadOptions))
		})
		It("validates missing backend group", func() {
			readOpts.Rabbit = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.Rabbit.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty exchange", func() {
			readOpts.Rabbit.Args.ExchangeName = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyExchangeName))
		})
		It("validates empty queue name", func() {
			readOpts.Rabbit.Args.QueueName = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyQueueName))
		})
		It("validates empty binding keu", func() {
			readOpts.Rabbit.Args.BindingKey = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyBindingKey))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			p := &RabbitMQ{}
			err := p.Read(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrMissingReadOptions.Error()))
		})

		It("reads a message", func() {
			fakeRabbit := &rabbitfakes.FakeIRabbit{}
			fakeRabbit.ConsumeStub = func(context.Context, chan *rabbit.ConsumeError, func(msg amqp.Delivery) error) {
				_, _, consumeFunc := fakeRabbit.ConsumeArgsForCall(0)
				consumeFunc(amqp.Delivery{
					Headers:   amqp.Table{"test": "value"},
					Timestamp: time.Time{},
				})
			}

			r := &RabbitMQ{
				client: fakeRabbit,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errorCh := make(chan *records.ErrorRecord, 1)
			resultsCh := make(chan *records.ReadRecord, 1)
			err := r.Read(context.Background(), readOpts, resultsCh, errorCh)

			Expect(err).ToNot(HaveOccurred())
			Expect(resultsCh).Should(Receive())
			Expect(errorCh).ShouldNot(Receive())
		})
	})
})

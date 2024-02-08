package rabbitmq

import (
	"context"
	"io/ioutil"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/rabbit"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/rabbitmq/rabbitfakes"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("RabbitMQ Backend", func() {
	var r *RabbitMQ
	var relayOpts *opts.RelayOptions

	BeforeEach(func() {
		r = &RabbitMQ{
			connArgs: &args.RabbitConn{
				Address: "amqp://localhost:5672",
			},
			client: &rabbitfakes.FakeIRabbit{},
			log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		relayOpts = &opts.RelayOptions{
			Rabbit: &opts.RelayGroupRabbitOptions{
				Args: &args.RabbitReadArgs{
					ExchangeName:   "test",
					QueueName:      "test",
					BindingKey:     "test",
					QueueExclusive: false,
					QueueDeclare:   false,
					QueueDurable:   false,
					AutoAck:        false,
					ConsumerTag:    "",
					QueueDelete:    false,
				},
			},
		}
	})

	Context("validateRelayOptions", func() {
		It("validates nil relay options", func() {
			err := validateRelayOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyRelayOpts))
		})
		It("validates missing backend group", func() {
			relayOpts.Rabbit = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			relayOpts.Rabbit.Args = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty exchange", func() {
			relayOpts.Rabbit.Args.ExchangeName = ""
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyExchangeName))
		})
		It("validates empty queue name", func() {
			relayOpts.Rabbit.Args.QueueName = ""
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyQueueName))
		})
		It("validates empty binding keu", func() {
			relayOpts.Rabbit.Args.BindingKey = ""
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyBindingKey))
		})
	})

	Context("Relay", func() {
		It("validates relay options", func() {
			err := r.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyRelayOpts.Error()))
		})

		It("relays a message", func() {
			fakeRabbit := &rabbitfakes.FakeIRabbit{}
			fakeRabbit.ConsumeStub = func(context.Context, chan *rabbit.ConsumeError, func(msg amqp.Delivery) error) {
				_, _, consumeFunc := fakeRabbit.ConsumeArgsForCall(0)
				consumeFunc(amqp.Delivery{
					Headers:   amqp.Table{"test": "value"},
					Timestamp: time.Time{},
					Body:      []byte(`testing`),
				})
			}

			r.client = fakeRabbit

			relayCh := make(chan interface{}, 1)
			errorCh := make(chan *records.ErrorRecord, 1)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				// Sleep so there's some for goroutine launch and channel send/receive
				time.Sleep(time.Millisecond * 500)
				cancel()
			}()

			err := r.Relay(ctx, relayOpts, relayCh, errorCh)

			Expect(err).ToNot(HaveOccurred())
			Expect(fakeRabbit.ConsumeCallCount()).To(Equal(1))
			Expect(errorCh).ToNot(Receive())
			Expect(relayCh).To(Receive())
		})
	})
})

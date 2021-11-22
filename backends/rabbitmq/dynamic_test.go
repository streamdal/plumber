package rabbitmq

import (
	"context"
	"io/ioutil"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/backends/rabbitmq/rabbitfakes"
	"github.com/batchcorp/plumber/dynamic/dynamicfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("RabbitMQ Backend", func() {
	var dynamicOpts *opts.DynamicOptions
	var fakeDynamic *dynamicfakes.FakeIDynamic

	BeforeEach(func() {
		dynamicOpts = &opts.DynamicOptions{
			Rabbit: &opts.DynamicGroupRabbitOptions{
				Args: &args.RabbitWriteArgs{
					ExchangeName: "testing",
					RoutingKey:   "testing",
				},
			},
		}

		fakeDynamic = &dynamicfakes.FakeIDynamic{}
		fakeDynamic.ReadStub = func() chan *events.Outbound {
			ch := make(chan *events.Outbound, 1)
			ch <- &events.Outbound{Blob: []byte(`testing`)}
			return ch
		}
	})

	Context("validateDynamicOptions", func() {
		It("validates nil dynamic options", func() {
			err := validateDynamicOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			dynamicOpts.Rabbit = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.Rabbit.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty routing key", func() {
			dynamicOpts.Rabbit.Args.RoutingKey = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyRoutingKey))
		})
		It("validates empty exchange name", func() {
			dynamicOpts.Rabbit.Args.ExchangeName = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyExchangeName))
		})
		It("passes validation", func() {
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Dynamic", func() {
		It("validates dynamic options", func() {

			err := (&RabbitMQ{}).Dynamic(context.Background(), nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
		})
	})

	It("returns an when publish fails", func() {
		testErr := errors.New("test err")

		fakeRabbit := &rabbitfakes.FakeIRabbit{}
		fakeRabbit.PublishStub = func(context.Context, string, []byte) error {
			return testErr
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Millisecond * 500)
			cancel()
		}()

		p := &RabbitMQ{
			client: fakeRabbit,
			log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}
		err := p.Dynamic(ctx, dynamicOpts, fakeDynamic)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(testErr.Error()))
		Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
		Expect(fakeRabbit.PublishCallCount()).To(Equal(1))
	})

	It("replays a message", func() {
		fakeRabbit := &rabbitfakes.FakeIRabbit{}
		fakeRabbit.PublishStub = func(context.Context, string, []byte) error {
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Millisecond * 500)
			cancel()
		}()

		p := &RabbitMQ{
			client: fakeRabbit,
			log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}
		err := p.Dynamic(ctx, dynamicOpts, fakeDynamic)

		Expect(err).ToNot(HaveOccurred())
		Expect(fakeDynamic.StartCallCount()).To(Equal(1))
		Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
		Expect(fakeRabbit.PublishCallCount()).To(Equal(1))
	})

})

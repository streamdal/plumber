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
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/rabbitmq/rabbitfakes"
	"github.com/streamdal/plumber/tunnel/tunnelfakes"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("RabbitMQ Backend", func() {
	var tunnelOpts *opts.TunnelOptions
	var fakeTunnel *tunnelfakes.FakeITunnel

	BeforeEach(func() {
		tunnelOpts = &opts.TunnelOptions{
			Rabbit: &opts.TunnelGroupRabbitOptions{
				Args: &args.RabbitWriteArgs{
					ExchangeName: "testing",
					RoutingKey:   "testing",
				},
			},
		}

		fakeTunnel = &tunnelfakes.FakeITunnel{}
		fakeTunnel.ReadStub = func() chan *events.Outbound {
			ch := make(chan *events.Outbound, 1)
			ch <- &events.Outbound{Blob: []byte(`testing`)}
			return ch
		}
	})

	Context("validateTunnelOptions", func() {
		It("validates nil tunnel options", func() {
			err := validateTunnelOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyTunnelOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.Rabbit = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.Rabbit.Args = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty routing key", func() {
			tunnelOpts.Rabbit.Args.RoutingKey = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyRoutingKey))
		})
		It("validates empty exchange name", func() {
			tunnelOpts.Rabbit.Args.ExchangeName = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyExchangeName))
		})
		It("passes validation", func() {
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Tunnel", func() {
		It("validates tunnel options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := (&RabbitMQ{}).Tunnel(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyTunnelOpts.Error()))
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

		errorCh := make(chan *records.ErrorRecord)
		err := p.Tunnel(ctx, tunnelOpts, fakeTunnel, errorCh)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(testErr.Error()))
		Expect(fakeTunnel.ReadCallCount()).To(Equal(1))
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

		errorCh := make(chan *records.ErrorRecord)
		err := p.Tunnel(ctx, tunnelOpts, fakeTunnel, errorCh)

		Expect(err).ToNot(HaveOccurred())
		Expect(fakeTunnel.StartCallCount()).To(Equal(1))
		Expect(fakeTunnel.ReadCallCount()).To(Equal(1))
		Expect(fakeRabbit.PublishCallCount()).To(Equal(1))
	})

})

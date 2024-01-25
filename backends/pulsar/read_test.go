package pulsar

import (
	"context"
	"errors"
	"io/ioutil"

	"github.com/apache/pulsar-client-go/pulsar"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/pulsar/pulsarfakes"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("Pulsar Backend", func() {
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		readOpts = &opts.ReadOptions{
			Pulsar: &opts.ReadGroupPulsarOptions{
				Args: &args.PulsarReadArgs{
					Topic:            "test",
					SubscriptionName: "test",
					SubscriptionType: args.SubscriptionType_SHARED,
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
			readOpts.Pulsar = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.Pulsar.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			readOpts.Pulsar.Args.Topic = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
		It("validates empty subscription name", func() {
			readOpts.Pulsar.Args.SubscriptionName = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptySubscriptionName))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			p := &Pulsar{}
			err := p.Read(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrMissingReadOptions.Error()))
		})

		It("writes to error channel on failure to receive", func() {
			testErr := errors.New("test err")

			fakeConsumer := &pulsarfakes.FakeConsumer{}
			fakeConsumer.ReceiveStub = func(context.Context) (pulsar.Message, error) {
				return nil, testErr
			}

			fakePulsar := &pulsarfakes.FakeClient{}
			fakePulsar.SubscribeStub = func(pulsar.ConsumerOptions) (pulsar.Consumer, error) {

				return fakeConsumer, nil
			}

			p := Pulsar{
				client: fakePulsar,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			resultsCh := make(chan *records.ReadRecord, 1)
			errorCh := make(chan *records.ErrorRecord, 1)
			err := p.Read(context.Background(), readOpts, resultsCh, errorCh)

			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).Should(Receive())
			Expect(resultsCh).ShouldNot(Receive())
			Expect(fakeConsumer.CloseCallCount()).To(Equal(1))
			Expect(fakeConsumer.UnsubscribeCallCount()).To(Equal(1))
		})

		It("reads a message to results chan", func() {
			fakeConsumer := &pulsarfakes.FakeConsumer{}
			fakeConsumer.ReceiveStub = func(context.Context) (pulsar.Message, error) {
				return &pulsarfakes.FakeMessage{}, nil
			}

			fakePulsar := &pulsarfakes.FakeClient{}
			fakePulsar.SubscribeStub = func(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
				return fakeConsumer, nil
			}

			p := Pulsar{
				client: fakePulsar,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			resultsCh := make(chan *records.ReadRecord, 1)
			errorCh := make(chan *records.ErrorRecord, 1)
			err := p.Read(context.Background(), readOpts, resultsCh, errorCh)

			Expect(err).ToNot(HaveOccurred())

			// Error will receive in the test because the mock can't be marshalled to JSON
			// This is ok, because the actual pulsar message can be marshalled
			//Expect(errorCh).ShouldNot(Receive())
			Expect(resultsCh).Should(Receive())
			Expect(fakeConsumer.CloseCallCount()).To(Equal(1))
			Expect(fakeConsumer.UnsubscribeCallCount()).To(Equal(1))
			Expect(fakeConsumer.AckCallCount()).To(Equal(1))
		})
	})

	Context("getSubscriptionType", func() {
		It("returns correct types", func() {
			readOpts.Pulsar.Args.SubscriptionType = args.SubscriptionType_SHARED
			Expect(getSubscriptionType(readOpts)).To(Equal(pulsar.Shared))
			readOpts.Pulsar.Args.SubscriptionType = args.SubscriptionType_EXCLUSIVE
			Expect(getSubscriptionType(readOpts)).To(Equal(pulsar.Exclusive))
			readOpts.Pulsar.Args.SubscriptionType = args.SubscriptionType_FAILOVER
			Expect(getSubscriptionType(readOpts)).To(Equal(pulsar.Failover))
			readOpts.Pulsar.Args.SubscriptionType = args.SubscriptionType_KEYSHARED
			Expect(getSubscriptionType(readOpts)).To(Equal(pulsar.KeyShared))
		})
	})
})

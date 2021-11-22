package pulsar

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends/pulsar/pulsarfakes"
	"github.com/batchcorp/plumber/dynamic/dynamicfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Pulsar Backend", func() {
	var dynamicOpts *opts.DynamicOptions

	BeforeEach(func() {
		dynamicOpts = &opts.DynamicOptions{
			Pulsar: &opts.DynamicGroupPulsarOptions{
				Args: &args.PulsarWriteArgs{
					Topic: "testing",
				},
			},
		}
	})

	Context("validateDynamicOptions", func() {
		It("validates nil dynamic options", func() {
			err := validateDynamicOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			dynamicOpts.Pulsar = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.Pulsar.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			dynamicOpts.Pulsar.Args.Topic = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
		It("passes validation", func() {
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Dynamic", func() {
		var fakeDynamic *dynamicfakes.FakeIDynamic

		BeforeEach(func() {
			fakeDynamic = &dynamicfakes.FakeIDynamic{}
			fakeDynamic.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}
		})

		It("validates dynamic options", func() {
			err := (&Pulsar{}).Dynamic(context.Background(), nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
		})

		It("returns error when producer fails to create", func() {
			testErr := errors.New("test err")

			fakeClient := &pulsarfakes.FakeClient{}
			fakeClient.CreateProducerStub = func(pulsar.ProducerOptions) (pulsar.Producer, error) {
				return nil, testErr
			}

			p := &Pulsar{
				client: fakeClient,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Dynamic(context.Background(), dynamicOpts, fakeDynamic)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(testErr.Error()))
		})

		It("returns an error when publish fails", func() {
			fakeProducer := &pulsarfakes.FakeProducer{}
			fakePulsar := &pulsarfakes.FakeClient{}
			fakePulsar.CreateProducerStub = func(pulsar.ProducerOptions) (pulsar.Producer, error) {
				fakeProducer.SendStub = func(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
					return nil, errors.New("test err")
				}
				return fakeProducer, nil
			}

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(time.Millisecond * 500)
				cancel()
			}()

			p := &Pulsar{
				client: fakePulsar,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Dynamic(ctx, dynamicOpts, fakeDynamic)

			// Allow start goroutine to launch
			time.Sleep(time.Millisecond * 100)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Unable to replay message"))
			Expect(fakeDynamic.StartCallCount()).To(Equal(1))
			Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
			Expect(fakeProducer.SendCallCount()).To(Equal(1))
		})

		It("replays a message", func() {
			fakeProducer := &pulsarfakes.FakeProducer{}
			fakePulsar := &pulsarfakes.FakeClient{}
			fakePulsar.CreateProducerStub = func(pulsar.ProducerOptions) (pulsar.Producer, error) {
				fakeProducer.SendStub = func(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
					return &pulsarfakes.FakeMessageID{}, nil
				}
				return fakeProducer, nil
			}

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(time.Millisecond * 500)
				cancel()
			}()

			p := &Pulsar{
				client: fakePulsar,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			err := p.Dynamic(ctx, dynamicOpts, fakeDynamic)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeDynamic.StartCallCount()).To(Equal(1))
			Expect(fakeDynamic.ReadCallCount()).To(Equal(1))
			Expect(fakeProducer.SendCallCount()).To(Equal(1))
		})
	})
})

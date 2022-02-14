package nats_streaming

import (
	"context"
	"errors"
	"io/ioutil"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/nats-streaming/stanfakes"
	"github.com/batchcorp/plumber/dynamic/dynamicfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Nats Streaming Backend", func() {
	var dynamicOpts *opts.DynamicOptions

	BeforeEach(func() {
		dynamicOpts = &opts.DynamicOptions{
			NatsStreaming: &opts.DynamicGroupNatsStreamingOptions{
				Args: &args.NatsStreamingWriteArgs{
					Channel: "testing",
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
			dynamicOpts.NatsStreaming = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.NatsStreaming.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			dynamicOpts.NatsStreaming.Args.Channel = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyChannel))
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
			errorCh := make(chan *records.ErrorRecord)
			err := (&NatsStreaming{}).Dynamic(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
		})

		It("returns an error on publish failure", func() {
			errTest := errors.New("test err")

			fakeStan := &stanfakes.FakeConn{}
			fakeStan.PublishStub = func(string, []byte) error {
				return errTest
			}

			n := &NatsStreaming{
				stanClient: fakeStan,
				log:        logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				time.Sleep(time.Millisecond * 500)
				cancel()
			}()

			errorCh := make(chan *records.ErrorRecord)
			err := n.Dynamic(ctx, dynamicOpts, fakeDynamic, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(errTest.Error()))
		})

		It("replays a message", func() {
			fakeStan := &stanfakes.FakeConn{}
			fakeStan.PublishStub = func(string, []byte) error {
				return nil
			}

			n := &NatsStreaming{
				stanClient: fakeStan,
				log:        logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				time.Sleep(time.Millisecond * 500)
				cancel()
			}()

			errorCh := make(chan *records.ErrorRecord)
			err := n.Dynamic(ctx, dynamicOpts, fakeDynamic, errorCh)
			Expect(err).ToNot(HaveOccurred())
		})
	})

})

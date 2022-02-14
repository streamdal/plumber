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
	"github.com/batchcorp/plumber/tunnel/tunnelfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Nats Streaming Backend", func() {
	var tunnelOpts *opts.DynamicOptions

	BeforeEach(func() {
		tunnelOpts = &opts.DynamicOptions{
			NatsStreaming: &opts.DynamicGroupNatsStreamingOptions{
				Args: &args.NatsStreamingWriteArgs{
					Channel: "testing",
				},
			},
		}
	})

	Context("validateTunnelOptions", func() {
		It("validates nil tunnel options", func() {
			err := validateTunnelOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.NatsStreaming = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.NatsStreaming.Args = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			tunnelOpts.NatsStreaming.Args.Channel = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyChannel))
		})
		It("passes validation", func() {
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Tunnel", func() {
		var fakeDynamic *tunnelfakes.FakeIDynamic

		BeforeEach(func() {
			fakeDynamic = &tunnelfakes.FakeIDynamic{}
			fakeDynamic.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}
		})

		It("validates tunnel options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := (&NatsStreaming{}).Tunnel(context.Background(), nil, nil, errorCh)
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
			err := n.Tunnel(ctx, tunnelOpts, fakeDynamic, errorCh)
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
			err := n.Tunnel(ctx, tunnelOpts, fakeDynamic, errorCh)
			Expect(err).ToNot(HaveOccurred())
		})
	})

})

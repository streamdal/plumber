package nats_streaming

import (
	"context"
	"io/ioutil"

	"github.com/nats-io/stan.go"
	pb2 "github.com/nats-io/stan.go/pb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/nats-streaming/stanfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Nats Streaming Backend", func() {
	var n *NatsStreaming
	var relayOpts *opts.RelayOptions

	BeforeEach(func() {
		n = &NatsStreaming{
			connArgs:   &args.NatsStreamingConn{},
			stanClient: &stanfakes.FakeConn{},
			log:        logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		relayOpts = &opts.RelayOptions{
			NatsStreaming: &opts.RelayGroupNatsStreamingOptions{
				Args: &args.NatsStreamingReadArgs{
					Channel: "test",
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
			relayOpts.NatsStreaming = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			relayOpts.NatsStreaming.Args = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			relayOpts.NatsStreaming.Args.Channel = ""
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyChannel))
		})
	})

	Context("Relay", func() {
		It("validates relay options", func() {
			err := n.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyRelayOpts.Error()))
		})

		It("relays a message", func() {
			ctx, cancel := context.WithCancel(context.Background())
			fakeStan := &stanfakes.FakeConn{}
			fakeStan.SubscribeStub = func(subject string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) (stan.Subscription, error) {
				sub := &stanfakes.FakeSubscription{}
				cb(&stan.Msg{
					MsgProto: pb2.MsgProto{
						Data: []byte("hello"),
					},
					Sub: nil,
				})

				cancel()

				return sub, nil
			}

			n.stanClient = fakeStan

			relayCh := make(chan interface{}, 1)
			errorCh := make(chan *records.ErrorRecord, 1)

			err := n.Relay(ctx, relayOpts, relayCh, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(relayCh).Should(Receive())
			Expect(fakeStan.SubscribeCallCount()).To(Equal(1))
		})
	})
})

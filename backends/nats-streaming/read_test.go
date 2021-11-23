package nats_streaming

import (
	"context"
	"io/ioutil"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	pb2 "github.com/nats-io/stan.go/pb"

	"github.com/nats-io/stan.go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends/nats-streaming/stanfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Nats Streaming Backend", func() {
	var n *NatsStreaming
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		n = &NatsStreaming{
			connArgs:   &args.NatsStreamingConn{},
			stanClient: &stanfakes.FakeConn{},
			log:        logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		readOpts = &opts.ReadOptions{
			Continuous: false,
			NatsStreaming: &opts.ReadGroupNatsStreamingOptions{
				Args: &args.NatsStreamingReadArgs{
					Channel:            "testing",
					DurableName:        "plumber",
					ReadLastAvailable:  false,
					ReadSequenceNumber: 0,
					ReadSince:          "",
					ReadAll:            false,
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
			readOpts.NatsStreaming = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.NatsStreaming.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty channel", func() {
			_ = n
			readOpts.NatsStreaming.Args.Channel = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyChannel))
		})

		It("validates read sequence number", func() {
			readOpts.NatsStreaming.Args.ReadSequenceNumber = 1
			readOpts.NatsStreaming.Args.ReadLastAvailable = true
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidReadOption))
		})

		It("validates read all", func() {
			readOpts.NatsStreaming.Args.ReadLastAvailable = true
			readOpts.NatsStreaming.Args.ReadAll = true
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidReadOption))
		})

		It("validates read since", func() {
			readOpts.NatsStreaming.Args.ReadLastAvailable = true
			readOpts.NatsStreaming.Args.ReadSince = "test"
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidReadOption))
		})

		It("passes validation", func() {
			readOpts.NatsStreaming.Args.ReadLastAvailable = true
			err := validateReadOptions(readOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("getReadOptions", func() {
		It("returns correct options for read all", func() {
			readOpts.NatsStreaming.Args.ReadAll = true
			opts, err := n.getReadOptions(readOpts)

			Expect(err).ToNot(HaveOccurred())
			// Options are closures, no way to test, so just check length
			Expect(len(opts)).To(Equal(2))
		})
		It("returns correct options for read last available", func() {
			readOpts.NatsStreaming.Args.ReadLastAvailable = true
			opts, err := n.getReadOptions(readOpts)

			Expect(err).ToNot(HaveOccurred())
			// Options are closures, no way to test, so just check length
			Expect(len(opts)).To(Equal(2))
		})
		It("returns correct options for read since", func() {
			readOpts.NatsStreaming.Args.ReadSince = "10m"
			opts, err := n.getReadOptions(readOpts)

			Expect(err).ToNot(HaveOccurred())
			// Options are closures, no way to test, so just check length
			Expect(len(opts)).To(Equal(2))
		})
		It("returns error for invalid read since", func() {
			readOpts.NatsStreaming.Args.ReadSince = "abc123"
			_, err := n.getReadOptions(readOpts)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to parse --since option"))
		})

		It("returns correct options for read sequence", func() {
			readOpts.NatsStreaming.Args.ReadSequenceNumber = 1
			opts, err := n.getReadOptions(readOpts)

			Expect(err).ToNot(HaveOccurred())
			// Options are closures, no way to test, so just check length
			Expect(len(opts)).To(Equal(2))
		})
		It("returns correct options for new-only", func() {
			opts, err := n.getReadOptions(readOpts)

			Expect(err).ToNot(HaveOccurred())
			// Options are closures, no way to test, so just check length
			Expect(len(opts)).To(Equal(2))
		})
	})

	Context("Read", func() {
		It("It reads a message", func() {
			fakeStan := &stanfakes.FakeConn{}
			fakeStan.SubscribeStub = func(subject string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) (stan.Subscription, error) {
				sub := &stanfakes.FakeSubscription{}
				cb(&stan.Msg{
					MsgProto: pb2.MsgProto{
						Data: []byte("hello"),
					},
					Sub: nil,
				})

				return sub, nil
			}

			n.stanClient = fakeStan

			readOpts.NatsStreaming.Args.ReadLastAvailable = true

			resultsCh := make(chan *records.ReadRecord, 1)
			errorCh := make(chan *records.ErrorRecord, 1)
			err := n.Read(context.Background(), readOpts, resultsCh, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(resultsCh).Should(Receive())
		})
	})
})

package nats_streaming

import (
	"context"
	"io/ioutil"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/nats-streaming/stanfakes"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Nats Streaming Backend", func() {
	var n *NatsStreaming
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		n = &NatsStreaming{
			connArgs:   &args.NatsStreamingConn{},
			stanClient: &stanfakes.FakeConn{},
			log:        logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		writeOpts = &opts.WriteOptions{
			NatsStreaming: &opts.WriteGroupNatsStreamingOptions{
				Args: &args.NatsStreamingWriteArgs{
					Channel: "test",
				},
			},
		}
	})

	Context("validateWriteOptions", func() {
		It("validates nil tunnel options", func() {
			err := validateWriteOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyWriteOpts))
		})
		It("validates nil backend group", func() {
			writeOpts.NatsStreaming = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			writeOpts.NatsStreaming.Args = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			writeOpts.NatsStreaming.Args.Channel = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyChannel))
		})
		It("passes validation", func() {
			err := validateWriteOptions(writeOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		It("validates write options", func() {
			err := n.Write(context.Background(), nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyWriteOpts.Error()))
		})
		It("writes to error channel on failure to publish", func() {
			fakeStan := &stanfakes.FakeConn{}
			fakeStan.PublishStub = func(string, []byte) error {
				return errors.New("test err")
			}

			n.stanClient = fakeStan

			errorCh := make(chan *records.ErrorRecord, 1)
			err := n.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: `test`})
			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).Should(Receive())
		})
		It("publishes a message", func() {
			fakeStan := &stanfakes.FakeConn{}
			fakeStan.PublishStub = func(string, []byte) error {
				return nil
			}

			n.stanClient = fakeStan

			errorCh := make(chan *records.ErrorRecord, 1)
			err := n.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: `test`})
			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).ShouldNot(Receive())
		})
	})

})

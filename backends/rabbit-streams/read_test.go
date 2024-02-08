package rabbit_streams

import (
	"context"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/validate"
)

var _ = Describe("Rabbit Streams Backend", func() {
	var m *RabbitStreams
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		m = &RabbitStreams{
			connArgs: &args.RabbitStreamsConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		readOpts = &opts.ReadOptions{
			RabbitStreams: &opts.ReadGroupRabbitStreamsOptions{
				Args: &args.RabbitStreamsReadArgs{
					Stream:            "test",
					DeclareStream:     true,
					DeclareStreamSize: "10mb",
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
			readOpts.RabbitStreams = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.RabbitStreams.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing stream", func() {
			readOpts.RabbitStreams.Args.Stream = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyStream))
		})
	})

	Context("validateDeclareStreamArgs", func() {
		It("returns nil if declare stream is false", func() {
			readOpts.RabbitStreams.Args.DeclareStream = false
			err := validateDeclareStreamArgs(readOpts.GetRabbitStreams().Args)
			Expect(err).ToNot(HaveOccurred())
		})

		It("validates missing declare stream size", func() {
			readOpts.RabbitStreams.Args.DeclareStream = true
			readOpts.RabbitStreams.Args.DeclareStreamSize = ""
			err := validateDeclareStreamArgs(readOpts.GetRabbitStreams().Args)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyStreamSize))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			err := m.Read(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrMissingReadOptions.Error()))
		})
	})
})

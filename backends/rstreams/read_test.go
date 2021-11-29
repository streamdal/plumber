package rstreams

import (
	"context"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Redis Streams Backend", func() {
	var m *RedisStreams
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		m = &RedisStreams{
			connArgs: &args.RedisStreamsConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		readOpts = &opts.ReadOptions{
			RedisStreams: &opts.ReadGroupRedisStreamsOptions{
				Args: &args.RedisStreamsReadArgs{
					Stream: []string{"test"},
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
			readOpts.RedisStreams = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.RedisStreams.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing backend stream", func() {
			readOpts.RedisStreams.Args.Stream = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingStream))
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

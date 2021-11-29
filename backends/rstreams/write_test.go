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
	var r *RedisStreams
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		r = &RedisStreams{
			connArgs: &args.RedisStreamsConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		writeOpts = &opts.WriteOptions{
			RedisStreams: &opts.WriteGroupRedisStreamsOptions{
				Args: &args.RedisStreamsWriteArgs{
					Streams: []string{"test"},
				},
			},
		}
	})

	Context("validateWriteOptions", func() {
		It("validates nil writes options", func() {
			err := validateWriteOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyWriteOpts))
		})
		It("validates nil backend group", func() {
			writeOpts.RedisStreams = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			writeOpts.RedisStreams.Args = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
	})

	Context("Write", func() {
		It("validates write options", func() {
			err := r.Write(context.Background(), nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyWriteOpts.Error()))
		})

		It("Publishes message", func() {
			// Not tested due to lack of interface
		})
	})
})

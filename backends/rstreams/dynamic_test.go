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
	var dynamicOpts *opts.DynamicOptions

	BeforeEach(func() {
		r = &RedisStreams{
			connArgs: &args.RedisStreamsConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		dynamicOpts = &opts.DynamicOptions{
			RedisStreams: &opts.DynamicGroupRedisStreamsOptions{
				Args: &args.RedisStreamsWriteArgs{
					Stream: []string{"test"},
				},
			},
		}
	})

	Context("validateDynamicOptions", func() {
		It("validates nil writes options", func() {
			err := validateDynamicOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			dynamicOpts.RedisStreams = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.RedisStreams.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing stream", func() {
			dynamicOpts.RedisStreams.Args.Stream = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingStream))
		})
	})

	Context("Dynamic", func() {
		It("validates dynamic options", func() {
			err := r.Dynamic(context.Background(), nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
		})

		It("Publishes message", func() {
			// Not tested due to lack of interface
		})
	})
})

package rstreams

import (
	"context"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Redis Streams Backend", func() {
	var r *RedisStreams
	var tunnelOpts *opts.DynamicOptions

	BeforeEach(func() {
		r = &RedisStreams{
			connArgs: &args.RedisStreamsConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		tunnelOpts = &opts.DynamicOptions{
			RedisStreams: &opts.DynamicGroupRedisStreamsOptions{
				Args: &args.RedisStreamsWriteArgs{
					Streams: []string{"test"},
				},
			},
		}
	})

	Context("validateTunnelOptions", func() {
		It("validates nil writes options", func() {
			err := validateTunnelOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.RedisStreams = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.RedisStreams.Args = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing stream", func() {
			tunnelOpts.RedisStreams.Args.Streams = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingStream))
		})
	})

	Context("Tunnel", func() {
		It("validates tunnel options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := r.Tunnel(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyDynamicOpts.Error()))
		})

		It("Publishes message", func() {
			// Not tested due to lack of interface
		})
	})
})

package rpubsub

import (
	"context"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/validate"
)

var _ = Describe("Redis PubSub Backend", func() {
	var r *RedisPubsub
	var tunnelOpts *opts.TunnelOptions

	BeforeEach(func() {
		r = &RedisPubsub{
			connArgs: &args.RedisPubSubConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		tunnelOpts = &opts.TunnelOptions{
			RedisPubsub: &opts.TunnelGroupRedisPubSubOptions{
				Args: &args.RedisPubSubWriteArgs{
					Channels: []string{"test"},
				},
			},
		}
	})

	Context("validateTunnelOpts", func() {
		It("validates nil tunnel options", func() {
			err := validateTunnelOpts(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyTunnelOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.RedisPubsub = nil
			err := validateTunnelOpts(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.RedisPubsub.Args = nil
			err := validateTunnelOpts(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
	})

	Context("Tunnel", func() {
		It("validates dynaqmic options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := r.Tunnel(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyTunnelOpts.Error()))
		})

		It("Publishes message", func() {
			// Not tested due to lack non-exported struct returns
		})
	})
})

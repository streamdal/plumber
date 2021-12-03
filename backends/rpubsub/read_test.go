package rpubsub

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

var _ = Describe("Redis PubSub Backend", func() {
	var r *RedisPubsub
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		r = &RedisPubsub{
			connArgs: &args.RedisPubSubConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		readOpts = &opts.ReadOptions{
			RedisPubsub: &opts.ReadGroupRedisPubSubOptions{
				Args: &args.RedisPubSubReadArgs{
					Channels: []string{"test"},
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
			readOpts.RedisPubsub = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.RedisPubsub.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing backend channel", func() {
			readOpts.RedisPubsub.Args.Channels = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingChannel))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			err := r.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyRelayOpts.Error()))
		})
	})
})

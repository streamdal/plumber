package rstreams

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

var _ = Describe("Redis Streams Backend", func() {
	var r *RedisStreams
	var relayOpts *opts.RelayOptions

	BeforeEach(func() {
		r = &RedisStreams{
			connArgs: &args.RedisStreamsConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		relayOpts = &opts.RelayOptions{
			RedisStreams: &opts.RelayGroupRedisStreamsOptions{
				Args: &args.RedisStreamsReadArgs{
					Streams: []string{"test"},
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
			relayOpts.RedisStreams = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			relayOpts.RedisStreams.Args = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing backend stream", func() {
			relayOpts.RedisStreams.Args.Streams = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingStream))
		})
	})

	Context("Relay", func() {
		It("validates relay options", func() {
			err := r.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyRelayOpts.Error()))
		})
	})
})

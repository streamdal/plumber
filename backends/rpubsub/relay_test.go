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
	var m *RedisPubsub
	var relayOpts *opts.RelayOptions

	BeforeEach(func() {
		m = &RedisPubsub{
			connArgs: &args.RedisPubSubConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		relayOpts = &opts.RelayOptions{
			RedisPubsub: &opts.RelayGroupRedisPubSubOptions{
				Args: &args.RedisPubSubReadArgs{
					Channel: []string{"test"},
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
			relayOpts.RedisPubsub = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			relayOpts.RedisPubsub.Args = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates missing backend channel", func() {
			relayOpts.RedisPubsub.Args.Channel = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingChannel))
		})
	})

	Context("Relay", func() {
		It("validates relay options", func() {
			err := m.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyRelayOpts.Error()))
		})
	})
})

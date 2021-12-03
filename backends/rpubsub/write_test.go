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
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		r = &RedisPubsub{
			connArgs: &args.RedisPubSubConn{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		writeOpts = &opts.WriteOptions{
			RedisPubsub: &opts.WriteGroupRedisPubSubOptions{
				Args: &args.RedisPubSubWriteArgs{
					Channels: []string{"test"},
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
			writeOpts.RedisPubsub = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			writeOpts.RedisPubsub.Args = nil
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
			// Not tested due to lack non-exported struct returns
		})
	})
})

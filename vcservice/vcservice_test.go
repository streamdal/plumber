package vcservice

import (
	"sync"

	"github.com/batchcorp/plumber/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("vcservice", func() {
	Context("validateConfig", func() {
		It("fails when opts is nil", func() {
			err := validateConfig(&Config{})

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingServerOptions))
		})

		It("returns error on missing etcdservice", func() {
			err := validateConfig(&Config{
				EtcdService:      nil,
				ServerOptions:    &opts.ServerOptions{},
				PersistentConfig: &config.Config{},
			})

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingEtcd))
		})

		It("returns error on missing server options", func() {
			err := validateConfig(&Config{
				EtcdService:      &etcdfakes.FakeIEtcd{},
				ServerOptions:    nil,
				PersistentConfig: &config.Config{},
			})

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingServerOptions))
		})

		It("returns error on missing persistent config", func() {
			err := validateConfig(&Config{
				EtcdService:      &etcdfakes.FakeIEtcd{},
				ServerOptions:    &opts.ServerOptions{},
				PersistentConfig: nil,
			})

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConfig))
		})

		It("returns nil on valid options", func() {
			err := validateConfig(&Config{
				EtcdService:      &etcdfakes.FakeIEtcd{},
				ServerOptions:    &opts.ServerOptions{},
				PersistentConfig: &config.Config{},
			})
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("AttachStream", func() {
		It("Attaches a stream", func() {
			c := &Client{
				AttachedStreams:      make(map[string]*AttachedStream),
				AttachedStreamsMutex: &sync.RWMutex{},
			}

			c.AttachStream("testing")
			Expect(len(c.AttachedStreams)).To(Equal(1))
		})

		It("Detaches a stream", func() {
			c := &Client{
				AttachedStreams: map[string]*AttachedStream{
					"testing": {EventsCh: make(chan *protos.VCEvent)},
				},
				AttachedStreamsMutex: &sync.RWMutex{},
			}

			Expect(len(c.AttachedStreams)).To(Equal(1))

			c.DetachStream("testing")
			Expect(len(c.AttachedStreams)).To(Equal(0))
		})
	})
})

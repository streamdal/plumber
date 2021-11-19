package pulsar

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends/pulsar/pulsarfakes"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Pulsar Backend", func() {
	Context("Close", func() {
		It("calls close on the client", func() {
			fakeClient := &pulsarfakes.FakeClient{}

			p := &Pulsar{client: fakeClient}
			p.Close(nil)

			Expect(fakeClient.CloseCallCount()).To(Equal(1))
		})
	})

	Context("Test", func() {
		It("returns not implemented", func() {
			p := &Pulsar{}
			Expect(p.Test(nil)).To(Equal(types.NotImplementedErr))
		})
	})

	Context("validateBaseConnOpts", func() {
		It("validates conn presence", func() {
			err := validateBaseConnOpts(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnOpts))
		})
		It("validates conn config", func() {
			connOpts := &opts.ConnectionOptions{}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnCfg))
		})
		It("validates pulsar presence", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})
		It("validates DSN", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn: "",
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingDSN))
		})
		It("validates TLS key", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn:                   "test",
						TlsClientCert:         []byte(`test`),
						TlsClientKey:          nil,
						ConnectTimeoutSeconds: 1,
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingClientKey))
		})
		It("validates TLS cert", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn:                   "test",
						TlsClientCert:         nil,
						TlsClientKey:          []byte(`test`),
						ConnectTimeoutSeconds: 1,
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingClientCert))
		})
	})
})

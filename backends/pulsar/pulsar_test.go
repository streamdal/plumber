package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
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

	Context("Name", func() {
		It("returns backend name", func() {
			p := &Pulsar{}
			Expect(p.Name()).To(Equal("pulsar"))
		})
	})

	Context("getClientOptions", func() {
		It("handles TLS certs as strings", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn:                   "localhost",
						ConnectTimeoutSeconds: 1,
						TlsClientKey:          "---testcert---",
						TlsClientCert:         "---testcert---",
					},
				},
			}

			cfg := getClientOptions(connOpts)

			Expect(cfg).To(BeAssignableToTypeOf(&pulsar.ClientOptions{}))
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
						TlsClientCert:         "test",
						TlsClientKey:          "",
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
						TlsClientCert:         "",
						TlsClientKey:          "test",
						ConnectTimeoutSeconds: 1,
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingClientCert))
		})
		It("validates authentication conflict", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn:                   "test",
						TlsClientCert:         "test",
						TlsClientKey:          "test",
						Token:                 "test",
						ConnectTimeoutSeconds: 1,
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrAuthConflict))
		})
		It("validates connect timeout", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn:                   "test",
						TlsClientCert:         "",
						TlsClientKey:          "test",
						ConnectTimeoutSeconds: 0,
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrInvalidConnTimeout))
		})
		It("passes validation", func() {
			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Pulsar{
					Pulsar: &args.PulsarConn{
						Dsn:                   "test",
						TlsClientCert:         "test",
						TlsClientKey:          "test",
						ConnectTimeoutSeconds: 1,
					},
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})

package mqtt

import (
	"net/url"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/tools/mqttfakes"
	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("MQTT Backend", func() {
	var connOpts *opts.ConnectionOptions

	BeforeEach(func() {
		connOpts = &opts.ConnectionOptions{
			Conn: &opts.ConnectionOptions_Mqtt{
				Mqtt: &args.MQTTConn{
					Address:            "ssl://user:pass@localhost",
					ConnTimeoutSeconds: 1,
					ClientId:           "plumber",
					QosLevel:           0,
					TlsOptions: &args.MQTTTLSOptions{
						TlsClientCert: "../../test-assets/ssl/client.crt",
						TlsClientKey:  "../../test-assets/ssl/client.key",
						TlsCaCert:     "../../test-assets/ssl/ca.crt",
					},
				},
			},
		}
	})

	Context("Name", func() {
		It("returns backend name", func() {
			m := &MQTT{}
			Expect(m.Name()).To(Equal("mqtt"))
		})
	})

	Context("createClientOptions", func() {
		It("returns error on bad URL scheme", func() {
			uri, _ := url.Parse("http://localhost")

			_, err := createClientOptions(connOpts.GetMqtt(), uri)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("scheme must be ssl:// or tcp://"))
		})
		It("returns error on bad SSL config", func() {
			args := connOpts.GetMqtt()
			args.TlsOptions.TlsClientCert = args.TlsOptions.TlsClientKey
			uri, _ := url.Parse(args.Address)

			_, err := createClientOptions(connOpts.GetMqtt(), uri)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to generate TLS config"))
		})
		It("returns client options", func() {
			uri, _ := url.Parse(connOpts.GetMqtt().Address)

			clientOpts, err := createClientOptions(connOpts.GetMqtt(), uri)
			Expect(err).ToNot(HaveOccurred())
			Expect(clientOpts).To(BeAssignableToTypeOf(&pahomqtt.ClientOptions{}))
		})
	})

	Context("Close", func() {
		fakeMQTTClient := &mqttfakes.FakeClient{}
		m := &MQTT{client: fakeMQTTClient}
		err := m.Close(nil)

		Expect(err).To(BeNil())
		Expect(fakeMQTTClient.DisconnectCallCount()).To(Equal(1))
	})

	Context("Test", func() {
		It("returns not implemented", func() {
			err := (&MQTT{}).Test(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(types.NotImplementedErr))
		})
	})

	Context("validateBaseConnOpts", func() {
		It("validates conn presence", func() {
			err := validateBaseConnOpts(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnOpts))
		})
		It("validates conn config", func() {
			connOpts = &opts.ConnectionOptions{}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnCfg))
		})
		It("validates MQTT presence", func() {
			connOpts = &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Mqtt{
					Mqtt: nil,
				},
			}
			err := validateBaseConnOpts(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingConnArgs))
		})
		It("passes validation", func() {
			err := validateBaseConnOpts(connOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("New", func() {
		It("validates client options", func() {
			_, err := New(nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to validate options"))
		})
		It("returns error on invalid address", func() {
			connOpts.GetMqtt().Address = "$%#$#&"
			_, err := New(connOpts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to parse address"))
		})
	})
})

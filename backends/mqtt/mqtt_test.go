package mqtt

import (
	"crypto/tls"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/options"
)

var _ = Describe("MQTT Backend", func() {
	Context("createClientOptions", func() {
		It("returns error on invalid URI", func() {
			url, _ := url.Parse("http://localhost")

			_, err := createClientOptions(&options.Options{}, url)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errInvalidAddress))
		})

		It("returns a valid options struct", func() {
			opts := &options.Options{
				MQTT: &options.MQTTOptions{
					ClientID: "123",
				},
			}

			url, _ := url.Parse("tcp://user:pass@localhost")

			mqttOpts, err := createClientOptions(opts, url)

			Expect(err).ToNot(HaveOccurred())
			Expect(mqttOpts).ToNot(BeNil())
			Expect(mqttOpts.ClientID).To(Equal("123"))
			Expect(mqttOpts.Username).To(Equal("user"))
			Expect(mqttOpts.Password).To(Equal("pass"))
			Expect(mqttOpts.Servers[0].Scheme).To(Equal(url.Scheme))
			Expect(mqttOpts.Servers[0].Host).To(Equal(url.Host))
		})
	})

	Context("generateTLSConfig", func() {
		It("returns a valid tls.Config", func() {
			opts := &options.Options{
				MQTT: &options.MQTTOptions{
					TLSClientKeyFile:  "../../test-assets/ssl/client.key",
					TLSClientCertFile: "../../test-assets/ssl/client.crt",
					TLSCAFile:         "../../test-assets/ssl/ca.crt",
				},
			}

			tlsCfg, err := generateTLSConfig(opts)

			Expect(err).To(Not(HaveOccurred()))
			Expect(tlsCfg).To(BeAssignableToTypeOf(&tls.Config{}))
		})
	})
})

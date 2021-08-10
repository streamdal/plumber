package mqtt

import (
	"errors"
	"io/ioutil"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/tools/mqttfakes"
)

var _ = Describe("MQTT Write", func() {
	defer GinkgoRecover()

	logger := logrus.New()
	logger.Out = ioutil.Discard
	log := logrus.NewEntry(logger)

	var opts *options.Options

	BeforeEach(func() {
		opts = &opts.Options{
			MQTT: &opts.MQTTOptions{
				Address:           "ssl://localhost",
				Topic:             "testing",
				ClientID:          "123",
				TLSClientKeyFile:  "../../test-assets/ssl/client.key",
				TLSClientCertFile: "../../test-assets/ssl/client.crt",
				TLSCAFile:         "../../test-assets/ssl/ca.crt",
			},
		}
	})

	Context("validateWriteOptions", func() {
		It("returns error for invalid QOS", func() {
			opts.MQTT.Address = "tcp://localhost"
			opts.MQTT.QoSLevel = -1

			err := validateWriteOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errInvalidQOSLevel))
		})

		It("Returns err on missing --tls-client-cert-file flag", func() {
			opts.MQTT.TLSClientCertFile = ""

			err := validateWriteOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTlsCert))
		})

		It("Returns err on missing --tls-client-key-file flag", func() {
			opts.MQTT.TLSClientKeyFile = ""

			err := validateWriteOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTLSKey))
		})

		It("Returns err on missing --tls-ca-file flag", func() {
			opts.MQTT.TLSCAFile = ""

			err := validateWriteOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTLSCA))
		})

		It("Returns nil with all valid options", func() {
			err := validateWriteOptions(opts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		var m *MQTT

		BeforeEach(func() {
			m = &MQTT{
				Options: &opts.Options{
					MQTT: &opts.MQTTOptions{},
				},
				log: log,
			}
		})
		It("returns error on timeout", func() {
			fakeMqtt := &mqttfakes.FakeClient{}
			fakeMqtt.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					WaitTimeoutStub: func(_ time.Duration) bool {
						return false
					},
				}
			}

			m.client = fakeMqtt

			err := m.Write([]byte(`testing`))
			Expect(err).To(HaveOccurred())
			Expect(fakeMqtt.PublishCallCount()).To(Equal(1))
		})

		It("Returns error on publish failure", func() {
			fakeMqtt := &mqttfakes.FakeClient{}
			fakeMqtt.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					ErrorStub: func() error {
						return errors.New("test error")
					},
					WaitTimeoutStub: func(_ time.Duration) bool {
						return true
					},
				}
			}

			m.client = fakeMqtt

			err := m.Write([]byte(`testing`))
			Expect(err).To(HaveOccurred())
			Expect(fakeMqtt.PublishCallCount()).To(Equal(1))
		})

		It("returns nil on successful publish", func() {
			fakeMqtt := &mqttfakes.FakeClient{}
			fakeMqtt.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					ErrorStub: func() error {
						return nil
					},
					WaitTimeoutStub: func(_ time.Duration) bool {
						return true
					},
				}
			}

			m.client = fakeMqtt

			err := m.Write([]byte(`testing`))
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeMqtt.PublishCallCount()).To(Equal(1))
		})

	})
})

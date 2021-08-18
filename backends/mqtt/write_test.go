package mqtt

import (
	"context"
	"errors"
	"io/ioutil"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/tools/mqttfakes"
	"github.com/batchcorp/plumber/types"
)

var _ = Describe("MQTT Write", func() {
	defer GinkgoRecover()

	logger := logrus.New()
	logger.Out = ioutil.Discard
	log := logrus.NewEntry(logger)

	var opts *options.Options

	BeforeEach(func() {
		opts = &options.Options{
			MQTT: &options.MQTTOptions{
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
			opts.MQTT.Topic = "asdf"

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
				Options: &options.Options{
					MQTT: &options.MQTTOptions{},
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

			errorCh := make(chan *types.ErrorMessage, 1)

			m.Write(context.Background(), errorCh, &types.WriteMessage{
				Value: []byte(`testing`),
			})

			time.Sleep(time.Second) // error is written in a goroutine

			Expect(errorCh).Should(Receive())
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

			errorCh := make(chan *types.ErrorMessage, 1)

			m.Write(context.Background(), errorCh, &types.WriteMessage{
				Value: []byte(`testing`),
			})

			time.Sleep(time.Second)

			Expect(errorCh).Should(Receive())
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

			err := m.Write(context.Background(), nil, &types.WriteMessage{
				Value: []byte(`testing`),
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(fakeMqtt.PublishCallCount()).To(Equal(1))
		})

	})
})

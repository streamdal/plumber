package mqtt

import (
	"io/ioutil"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer/printerfakes"
	"github.com/batchcorp/plumber/tools/mqttfakes"
)

type message struct {
	duplicate bool
	qos       byte
	retained  bool
	topic     string
	messageID uint16
	payload   []byte
	once      sync.Once
	ack       func()
}

var _ = Describe("MQTT Read", func() {
	defer GinkgoRecover()

	logger := logrus.New()
	logger.Out = ioutil.Discard
	log := logrus.NewEntry(logger)

	var opts *cli.Options

	BeforeEach(func() {
		opts = &cli.Options{
			MQTT: &cli.MQTTOptions{
				Address:           "ssl://localhost",
				Topic:             "testing",
				ClientID:          "123",
				TLSClientKeyFile:  "../../test-assets/ssl/client.key",
				TLSClientCertFile: "../../test-assets/ssl/client.crt",
				TLSCAFile:         "../../test-assets/ssl/ca.crt",
			},
		}
	})

	Context("validateReadOptions", func() {
		It("Returns err on missing --address flag", func() {
			opts.MQTT.Address = ""

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingAddress))
		})

		It("Returns err on missing --topic flag", func() {
			opts.MQTT.Topic = ""

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTopic))
		})

		It("Returns err on missing --tls-client-cert-file flag", func() {
			opts.MQTT.TLSClientCertFile = ""

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTlsCert))
		})

		It("Returns err on missing --tls-client-key-file flag", func() {
			opts.MQTT.TLSClientKeyFile = ""

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTLSKey))
		})

		It("Returns err on missing --tls-ca-file flag", func() {
			opts.MQTT.TLSCAFile = ""

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTLSCA))
		})

		It("Returns err on invalid --qos flag value", func() {
			opts.MQTT.Address = "tcp://localhost"
			opts.MQTT.QoSLevel = -1

			err := validateReadOptions(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errInvalidQOSLevel))
		})
	})

	Context("Read", func() {
		fakeMQTT := &mqttfakes.FakeClient{}

		fakeMQTT.SubscribeStub = func(topic string, qos byte, handler mqtt.MessageHandler) mqtt.Token {
			msg := &mqttfakes.FakeMessage{}
			msg.PayloadStub = func() []byte {
				return []byte(`testing`)
			}

			handler(fakeMQTT, msg)

			return &mqttfakes.FakeToken{}
		}

		var readMessage string

		fakePrinter := &printerfakes.FakeIPrinter{}
		fakePrinter.PrintStub = func(str string) {
			readMessage = str
		}

		m := &MQTT{
			log:    log,
			Client: fakeMQTT,
			Options: &cli.Options{
				ReadFollow: false,
				MQTT:       &cli.MQTTOptions{},
			},
			printer: fakePrinter,
		}

		err := m.Read()

		Expect(err).ToNot(HaveOccurred())
		Expect(readMessage).To(Equal("1: testing"))
	})
})

package mqtt

import (
	"context"
	"errors"
	"io/ioutil"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/tools/mqttfakes"
	"github.com/streamdal/plumber/validate"
)

var _ = Describe("MQTT Backend", func() {
	var m *MQTT
	var readOpts *opts.ReadOptions

	BeforeEach(func() {
		m = &MQTT{
			connArgs: &args.MQTTConn{},
			client:   &mqttfakes.FakeClient{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		readOpts = &opts.ReadOptions{
			Mqtt: &opts.ReadGroupMQTTOptions{
				Args: &args.MQTTReadArgs{
					Topic:              "test",
					ReadTimeoutSeconds: 1,
				},
			},
		}
	})

	Context("validateReadOptions", func() {
		It("validates nil read options", func() {
			err := validateReadOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrMissingReadOptions))
		})
		It("validates missing backend group", func() {
			readOpts.Mqtt = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			readOpts.Mqtt.Args = nil
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			readOpts.Mqtt.Args.Topic = ""
			err := validateReadOptions(readOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
	})

	Context("Read", func() {
		It("validates read options", func() {
			m := &MQTT{}
			err := m.Read(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrMissingReadOptions.Error()))
		})

		It("returns an error when subscribe fails", func() {
			fakeMQTT := &mqttfakes.FakeClient{}
			fakeMQTT.SubscribeStub = func(topic string, qos byte, handler mqtt.MessageHandler) mqtt.Token {
				return &mqttfakes.FakeToken{
					ErrorStub: func() error {
						return errors.New("test error")
					},
				}
			}

			m.client = fakeMQTT

			resultsCh := make(chan *records.ReadRecord, 1)
			errorsCh := make(chan *records.ErrorRecord, 1)

			err := m.Read(context.Background(), readOpts, resultsCh, errorsCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("test error"))
		})

		It("reads a message", func() {
			fakeMQTT := &mqttfakes.FakeClient{}
			fakeMQTT.SubscribeStub = func(topic string, qos byte, handler mqtt.MessageHandler) mqtt.Token {
				msg := &mqttfakes.FakeMessage{}
				msg.PayloadStub = func() []byte {
					return []byte(`testing`)
				}

				handler(fakeMQTT, msg)

				return &mqttfakes.FakeToken{}
			}

			m.client = fakeMQTT

			resultsCh := make(chan *records.ReadRecord, 1)
			errorsCh := make(chan *records.ErrorRecord, 1)

			err := m.Read(context.Background(), readOpts, resultsCh, errorsCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(resultsCh).Should(Receive())
		})
	})
})

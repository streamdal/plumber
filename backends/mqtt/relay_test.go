package mqtt

import (
	"context"
	"io/ioutil"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tools/mqttfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("MQTT Backend", func() {
	var m *MQTT
	var relayOpts *opts.RelayOptions

	BeforeEach(func() {
		m = &MQTT{
			connArgs: &args.MQTTConn{},
			client:   &mqttfakes.FakeClient{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}

		relayOpts = &opts.RelayOptions{
			Mqtt: &opts.RelayGroupMQTTOptions{
				Args: &args.MQTTReadArgs{
					Topic:              "test",
					ReadTimeoutSeconds: 1,
				},
			},
		}
	})

	Context("validateRelayOptions", func() {
		It("validates nil relay options", func() {
			err := validateRelayOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyRelayOpts))
		})
		It("validates missing backend group", func() {
			relayOpts.Mqtt = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates missing backend args", func() {
			relayOpts.Mqtt.Args = nil
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			relayOpts.Mqtt.Args.Topic = ""
			err := validateRelayOptions(relayOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
	})

	Context("Relay", func() {
		It("validates relay options", func() {
			err := m.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyRelayOpts.Error()))
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

			relayCh := make(chan interface{}, 1)
			errorsCh := make(chan *records.ErrorRecord, 1)

			err := m.Relay(context.Background(), relayOpts, relayCh, errorsCh)
			Expect(err).To(HaveOccurred())
			Expect(fakeMQTT.SubscribeCallCount()).To(Equal(1))
		})

		It("relays a message", func() {
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

			relayCh := make(chan interface{}, 1)
			errorCh := make(chan *records.ErrorRecord, 1)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				time.Sleep(time.Millisecond * 200)
				cancel()
			}()

			err := m.Relay(ctx, relayOpts, relayCh, errorCh)

			Expect(err).ToNot(HaveOccurred())
			Expect(fakeMQTT.SubscribeCallCount()).To(Equal(1))
			Expect(relayCh).To(Receive())
		})
	})
})

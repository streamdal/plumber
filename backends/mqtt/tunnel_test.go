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

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tools/mqttfakes"

	"github.com/batchcorp/plumber/tunnel/tunnelfakes"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("MQTT Backend", func() {
	var tunnelOpts *opts.TunnelOptions

	BeforeEach(func() {
		tunnelOpts = &opts.TunnelOptions{
			Mqtt: &opts.TunnelGroupMQTTOptions{
				Args: &args.MQTTWriteArgs{
					Topic:               "test",
					WriteTimeoutSeconds: 1,
				},
			},
		}
	})

	Context("validateTunnelOptions", func() {
		It("validates nil tunnel options", func() {
			err := validateTunnelOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyTunnelOpts))
		})
		It("validates nil backend group", func() {
			tunnelOpts.Mqtt = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			tunnelOpts.Mqtt.Args = nil
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			tunnelOpts.Mqtt.Args.Topic = ""
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
		It("passes validation", func() {
			err := validateTunnelOptions(tunnelOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Tunnel", func() {
		var fakeTunnel *tunnelfakes.FakeITunnel

		BeforeEach(func() {
			fakeTunnel = &tunnelfakes.FakeITunnel{}
			fakeTunnel.ReadStub = func() chan *events.Outbound {
				ch := make(chan *events.Outbound, 1)
				ch <- &events.Outbound{Blob: []byte(`testing`)}
				return ch
			}
		})

		It("validates tunnel options", func() {
			errorCh := make(chan *records.ErrorRecord)
			err := (&MQTT{}).Tunnel(context.Background(), nil, nil, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyTunnelOpts.Error()))
		})

		It("returns an error on publish timeout", func() {
			fakeMQTT := &mqttfakes.FakeClient{}
			fakeMQTT.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					WaitTimeoutStub: func(_ time.Duration) bool {
						return false
					},
				}
			}

			m := &MQTT{
				client:   fakeMQTT,
				connArgs: &args.MQTTConn{},
				log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errorCh := make(chan *records.ErrorRecord)
			err := m.Tunnel(context.Background(), tunnelOpts, fakeTunnel, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("timed out"))
		})

		It("returns an error when publish fails", func() {
			fakeMQTT := &mqttfakes.FakeClient{}
			fakeMQTT.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					ErrorStub: func() error {
						return errors.New("test error")
					},
					WaitTimeoutStub: func(_ time.Duration) bool {
						return true
					},
				}
			}

			m := &MQTT{
				client:   fakeMQTT,
				connArgs: &args.MQTTConn{},
				log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errorCh := make(chan *records.ErrorRecord)
			err := m.Tunnel(context.Background(), tunnelOpts, fakeTunnel, errorCh)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to replay message"))
		})

		It("replays a message", func() {
			fakeMQTT := &mqttfakes.FakeClient{}
			fakeMQTT.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					ErrorStub:       func() error { return nil },
					WaitTimeoutStub: func(_ time.Duration) bool { return true },
				}
			}

			m := &MQTT{
				client:   fakeMQTT,
				connArgs: &args.MQTTConn{},
				log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(time.Second)
				cancel()
			}()

			errorCh := make(chan *records.ErrorRecord)
			err := m.Tunnel(ctx, tunnelOpts, fakeTunnel, errorCh)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeTunnel.StartCallCount()).To(Equal(1))
			Expect(fakeTunnel.ReadCallCount()).To(Equal(1))
			Expect(fakeMQTT.PublishCallCount()).To(Equal(1))
		})

	})
})

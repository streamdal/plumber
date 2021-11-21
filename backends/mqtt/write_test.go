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
	var writeOpts *opts.WriteOptions

	BeforeEach(func() {
		writeOpts = &opts.WriteOptions{
			Mqtt: &opts.WriteGroupMQTTOptions{
				Args: &args.MQTTWriteArgs{
					Topic:               "test",
					WriteTimeoutSeconds: 1,
				},
			},
		}

		m = &MQTT{
			connArgs: &args.MQTTConn{},
			client:   &mqttfakes.FakeClient{},
			log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}
	})

	Context("validateWriteOptions", func() {
		It("validates nil write options", func() {
			err := validateWriteOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyWriteOpts))
		})
		It("validates nil backend group", func() {
			writeOpts.Mqtt = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates nil backend args", func() {
			writeOpts.Mqtt.Args = nil
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			writeOpts.Mqtt.Args.Topic = ""
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
		It("validates write timeout", func() {
			writeOpts.Mqtt.Args.WriteTimeoutSeconds = 0
			err := validateWriteOptions(writeOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrWriteTimeout))
		})
		It("passes validation", func() {
			err := validateWriteOptions(writeOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Write", func() {
		It("validates write options", func() {
			m := &MQTT{}
			err := m.Write(nil, nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrEmptyWriteOpts.Error()))
		})

		It("returns an error on timeout", func() {
			fakeMqtt := &mqttfakes.FakeClient{}
			fakeMqtt.PublishStub = func(string, byte, bool, interface{}) mqtt.Token {
				return &mqttfakes.FakeToken{
					WaitTimeoutStub: func(_ time.Duration) bool {
						return false
					},
				}
			}

			m.client = fakeMqtt

			errorCh := make(chan *records.ErrorRecord, 1)
			err := m.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: "test"})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("timed out attempting to publish message"))
		})

		It("errors on failure to publish", func() {
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
			errorCh := make(chan *records.ErrorRecord, 1)
			err := m.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: "test"})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to complete publish"))
		})

		It("publishes a message", func() {
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
			errorCh := make(chan *records.ErrorRecord, 1)
			err := m.Write(context.Background(), writeOpts, errorCh, &records.WriteRecord{Input: "test"})

			Expect(err).ToNot(HaveOccurred())
		})
	})
})

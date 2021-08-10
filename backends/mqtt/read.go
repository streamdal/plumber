package mqtt

import (
	"fmt"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

// Read is the entry point function for performing read operations in MQTT.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &MQTT{
		Options: opts,
		client:  client,
		msgDesc: md,
		printer: printer.New(),
		log:     logrus.WithField("pkg", "mqtt/read.go"),
	}

	return r.Read()
}

func (m *MQTT) Read() error {
	defer m.client.Disconnect(0)

	m.log.Infof("Listening for message(s) on topic '%s' as clientId '%s'",
		m.Options.MQTT.Topic, m.Options.MQTT.ClientID)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error, 1)

	m.subscribe(wg, errChan)

	wg.Wait()

	err, closed := <-errChan
	if closed {
		return nil
	}

	return err
}

func (m *MQTT) subscribe(wg *sync.WaitGroup, errChan chan error) {
	count := 1

	m.client.Subscribe(m.Options.MQTT.Topic, byte(m.Options.MQTT.QoSLevel), func(client mqtt.Client, msg mqtt.Message) {
		data, err := reader.Decode(m.Options, m.msgDesc, msg.Payload())

		if err != nil {
			if !m.Options.ReadFollow {
				errChan <- fmt.Errorf("unable to complete conversion: %s", err)
				wg.Done()
				return
			}

			return
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		m.printer.Print(str)

		if !m.Options.ReadFollow {
			m.log.Debug("--follow NOT specified, stopping listen")

			close(errChan)
			wg.Done()
		}
	})
}

func validateReadOptions(opts *options.Options) error {
	if opts.MQTT.Address == "" {
		return errMissingAddress
	}

	if opts.MQTT.Topic == "" {
		return errMissingTopic
	}

	if strings.HasPrefix(opts.MQTT.Address, "ssl") {
		if opts.MQTT.TLSClientKeyFile == "" {
			return errMissingTLSKey
		}

		if opts.MQTT.TLSClientCertFile == "" {
			return errMissingTlsCert
		}

		if opts.MQTT.TLSCAFile == "" {
			return errMissingTLSCA
		}
	}

	if opts.MQTT.QoSLevel > 2 || opts.MQTT.QoSLevel < 0 {
		return errInvalidQOSLevel
	}

	return nil
}

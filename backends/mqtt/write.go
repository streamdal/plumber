package mqtt

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in MQTT.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &MQTT{
		Options: opts,
		Client:  client,
		log:     logrus.WithField("pkg", "mqtt/write.go"),
	}

	msg, err := writer.GenerateWriteValue(opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return r.Write(msg)
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (m *MQTT) Write(value []byte) error {
	m.log.Infof("Sending message to broker on topic '%s' as clientId '%s'",
		m.Options.MQTT.Topic, m.Options.MQTT.ClientId)

	token := m.Client.Publish(m.Options.MQTT.Topic, byte(m.Options.MQTT.QoSLevel), false, value)

	if !token.WaitTimeout(m.Options.MQTT.WriteTimeout) {
		return fmt.Errorf("timed out attempting to publish message after %s", m.Options.MQTT.WriteTimeout)
	}

	if token.Error() != nil {
		return errors.Wrap(token.Error(), "unable to complete publish")
	}

	return nil
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.MQTT.QoSLevel > 2 || opts.MQTT.QoSLevel < 0 {
		return errors.New("QoS level can only be 0, 1 or 2")
	}

	if strings.HasPrefix(opts.MQTT.Address, "ssl") {
		if opts.MQTT.TLSClientKeyFile == "" {
			return errors.New("--tls-client-key-file cannot be blank if using ssl")
		}

		if opts.MQTT.TLSClientCertFile == "" {
			return errors.New("--tls-client-cert-file cannot be blank if using ssl")
		}

		if opts.MQTT.TLSCAFile == "" {
			return errors.New("--tls-ca-file cannot be blank if using ssl")
		}
	}

	return nil
}

package mqtt

import (
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in MQTT.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (m *MQTT) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteMessageFromOptions(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &MQTT{
		Options: opts,
		client:  client,
		msgDesc: md,
		log:     logrus.WithField("pkg", "mqtt/write.go"),
	}

	defer client.Disconnect(0)

	for _, value := range writeValues {
		if err := r.Write(value); err != nil {
			r.log.Error(err)
		}
	}

	return nil
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (m *MQTT) Write(value []byte) error {
	m.log.Infof("Sending message to broker on topic '%s' as clientId '%s'",
		m.Options.MQTT.Topic, m.Options.MQTT.ClientID)

	token := m.client.Publish(m.Options.MQTT.Topic, byte(m.Options.MQTT.QoSLevel), false, value)

	if !token.WaitTimeout(m.Options.MQTT.WriteTimeout) {
		return fmt.Errorf("timed out attempting to publish message after %s", m.Options.MQTT.WriteTimeout)
	}

	if token.Error() != nil {
		return errors.Wrap(token.Error(), "unable to complete publish")
	}

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	if opts.MQTT.QoSLevel > 2 || opts.MQTT.QoSLevel < 0 {
		return errInvalidQOSLevel
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

	return nil
}

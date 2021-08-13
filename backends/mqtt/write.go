package mqtt

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// Write is the entry point function for performing write operations in MQTT.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (m *MQTT) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := validateWriteOptions(m.Options); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	// TODO: Make use of ctx
	for _, msg := range messages {
		if err := m.write(msg.Value); err != nil {
			util.WriteError(m.log, errorCh, err)
		}
	}

	return nil
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (m *MQTT) write(value []byte) error {
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

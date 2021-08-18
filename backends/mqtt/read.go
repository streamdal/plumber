package mqtt

import (
	"context"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

func (m *MQTT) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(m.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	m.log.Infof("Listening for message(s) on topic '%s' as clientId '%s'",
		m.Options.MQTT.Topic, m.Options.MQTT.ClientID)

	count := 1

	m.client.Subscribe(m.Options.MQTT.Topic, byte(m.Options.MQTT.QoSLevel), func(client mqtt.Client, msg mqtt.Message) {
		resultsChan <- &types.ReadMessage{
			Value:      msg.Payload(),
			ReceivedAt: time.Now().UTC(),
			Num:        count,
			Raw:        msg,
		}

		count++

		if !m.Options.Read.Follow {
			m.log.Debug("--follow NOT specified, stopping listen")

			m.client.Unsubscribe(m.Options.MQTT.Topic)
		}
	})

	return nil
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

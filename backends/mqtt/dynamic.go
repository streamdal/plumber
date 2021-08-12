package mqtt

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (m *MQTT) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "mqtt/dynamic")

	// Start up dynamic connection
	grpc, err := dproxy.New(m.Options, "MQTT")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			token := m.client.Publish(m.Options.MQTT.Topic, byte(m.Options.MQTT.QoSLevel), false, outbound.Blob)

			if !token.WaitTimeout(m.Options.MQTT.WriteTimeout) {
				llog.Errorf("timed out attempting to publish message after %s", m.Options.MQTT.WriteTimeout)
				break
			}

			if token.Error() != nil {
				llog.Errorf("unable to replay message: %s", token.Error())
				break
			}

			llog.Debugf("Replayed message to MQTT topic '%s' for replay '%s'", m.Options.MQTT.Topic, outbound.ReplayId)
		case <-ctx.Done():
			m.log.Warn("context done")
			break MAIN
		}
	}

	m.log.Debug("exiting")

	return nil
}

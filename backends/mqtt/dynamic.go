package mqtt

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	llog := logrus.WithField("pkg", "mqtt/dynamic")

	// Start up writer
	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to connect to MQTT")
	}

	defer client.Disconnect(0)

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "MQTT")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			token := client.Publish(opts.MQTT.Topic, byte(opts.MQTT.QoSLevel), false, outbound.Blob)

			if !token.WaitTimeout(opts.MQTT.WriteTimeout) {
				llog.Errorf("timed out attempting to publish message after %s", opts.MQTT.WriteTimeout)
				break
			}

			if token.Error() != nil {
				llog.Errorf("unable to replay message: %s", token.Error())
				break
			}

			llog.Debugf("Replayed message to MQTT topic '%s' for replay '%s'", opts.MQTT.Topic, outbound.ReplayId)
		}
	}
}

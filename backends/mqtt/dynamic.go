package mqtt

import (
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/writer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	log := logrus.WithField("pkg", "mqtt/dynamic")

	// Start up writer
	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to connect to MQTT")
	}

	defer client.Disconnect(0)

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "mqtt")
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
				log.Errorf("timed out attempting to publish message after %s", opts.MQTT.WriteTimeout)
			}

			if token.Error() != nil {
				log.Errorf("unable to replay message to MQTT: %s", token.Error())
			}

			log.Debugf("Replayed message to MQTT topic '%s' for replay '%s'", opts.MQTT.Topic, outbound.ReplayId)
		}
	}
}

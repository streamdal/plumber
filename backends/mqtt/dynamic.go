package mqtt

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (m *MQTT) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := logrus.WithField("pkg", "mqtt/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(dynamicOpts, "MQTT")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	timeout := util.DurationSec(dynamicOpts.Mqtt.Args.WriteTimeoutSeconds)
	topic := dynamicOpts.Mqtt.Args.Topic

	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			token := m.client.Publish(topic, byte(int(m.connArgs.QosLevel)), false, outbound.Blob)

			if !token.WaitTimeout(timeout) {
				return fmt.Errorf("timed out attempting to publish message after %d seconds",
					dynamicOpts.Mqtt.Args.WriteTimeoutSeconds)
			}

			if token.Error() != nil {
				return errors.Wrap(token.Error(), "unable to replay message")
			}

			llog.Debugf("Replayed message to MQTT topic '%s' for replay '%s'", topic, outbound.ReplayId)
		case <-ctx.Done():
			m.log.Warning("context cancelled")
			return nil
		}
	}

	return nil

}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Mqtt == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.Mqtt.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.Mqtt.Args.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}

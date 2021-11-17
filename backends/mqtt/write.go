package mqtt

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
)

func (m *MQTT) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.New("unable to validate write options")
	}

	timeout := util.DurationSec(writeOpts.Mqtt.Args.WriteTimeoutSeconds)

	for _, msg := range messages {
		token := m.client.Publish(writeOpts.Mqtt.Args.Topic, byte(int(m.connArgs.QosLevel)), false, msg.Input)

		if !token.WaitTimeout(timeout) {
			return fmt.Errorf("timed out attempting to publish message after %d seconds", writeOpts.Mqtt.Args.WriteTimeoutSeconds)
		}

		if token.Error() != nil {
			return errors.Wrap(token.Error(), "unable to complete publish")
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return errors.New("write options cannot be nil")
	}

	if writeOpts.Mqtt == nil {
		return errors.New("backend group options cannot be nil")
	}

	if writeOpts.Mqtt.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if writeOpts.Mqtt.Args.Topic == "" {
		return errors.New("Topic cannot be empty")
	}

	return nil
}

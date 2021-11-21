package mqtt

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (m *MQTT) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	timeout := util.DurationSec(writeOpts.Mqtt.Args.WriteTimeoutSeconds)

	for _, msg := range messages {
		token := m.client.Publish(writeOpts.Mqtt.Args.Topic, byte(int(m.connArgs.QosLevel)), false, msg.Input)

		if !token.WaitTimeout(timeout) {
			err := fmt.Errorf("timed out attempting to publish message after %d seconds", writeOpts.Mqtt.Args.WriteTimeoutSeconds)
			util.WriteError(m.log, errorCh, err)
			return err
		}

		if token.Error() != nil {
			err := errors.Wrap(token.Error(), "unable to complete publish")
			util.WriteError(m.log, errorCh, err)
			return err
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.Mqtt == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.Mqtt.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Topic == "" {
		return ErrEmptyTopic
	}

	if args.WriteTimeoutSeconds <= 0 {
		return ErrWriteTimeout
	}

	return nil
}

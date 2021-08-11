package activemq

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/writer"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Write(ctx context.Context) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	if err := writer.ValidateWriteOptions(a.Options, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(a.Options.Encoding.MsgDesc, a.Options)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	for _, value := range writeValues {
		if err := a.write(value); err != nil {
			a.log.Error(err)
		}
	}

	return nil
}

// Write writes a message to an ActiveMQ topic
func (a *ActiveMq) write(value []byte) error {
	if err := a.client.Send(a.getDestination(), "", value, nil); err != nil {
		a.log.Infof("Unable to write message to '%s': %s", a.getDestination(), err)
		return errors.Wrap(err, "unable to write message")
	}

	a.log.Infof("Successfully wrote message to '%s'", a.getDestination())

	return nil
}

package activemq

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if a.client == nil {
		return types.BackendNotConnectedErr
	}

	for _, msg := range messages {
		if err := a.validateWriteMessage(msg); err != nil {
			util.WriteError(a.log, errorCh, errors.Wrap(err, "unable to validate message"))
			continue
		}

		if err := a.write(msg.Value); err != nil {
			util.WriteError(a.log, errorCh, err)
			continue
		}
	}

	return nil
}

func (a *ActiveMq) validateWriteMessage(m *types.WriteMessage) error {
	if m == nil {
		return errors.New("message cannot be nil")
	}

	if len(m.Value) == 0 {
		return errors.New("value cannot be empty")
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

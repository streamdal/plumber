package server

import (
	"context"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber/types"
)

// startBackgroundRead is a goroutine that is launched when a read is started.
// It will continue running until plumber exits or a read is stopped via the API
func (r *Read) startBackgroundRead() {
	defer func() {
		if err := r.Backend.Close(context.Background()); err != nil {
			r.log.Warnf("unable to close connection to backend in startBackgroundRead defer: %s", err)
		}
	}()

	r.Config.Active = true

	resultChan := make(chan *types.ReadMessage, 1)
	errorChan := make(chan *types.ErrorMessage, 1)

	// Launch backend reader
	go func() {
		r.log.Debug("goroutine launched for backed.Read()")

		if err := r.Backend.Read(r.ContextCxl, resultChan, errorChan); err != nil {
			if err == context.Canceled {
				r.log.Debug("backend read cancelled")
				return
			}

			r.log.Errorf("error during backend.Read(): %s", err)
		}

		r.log.Debug("goroutine exiting for backed.Read()")
	}()

	// Listen for results and pump them to attached clients
MAIN:
	for {
		var err error

		select {
		case <-r.ContextCxl.Done():
			r.log.Info("startBackgroundRead stopped")
			break MAIN
		case resultMsg := <-resultChan:
			err = r.processResultMessage(resultMsg)
		case errorMsg := <-errorChan:
			// TODO: We should probably send the error message to attached clients
			// PUNT for now; ds & mg 08.20.21
			r.log.Errorf("received error message for read '%s': %+v", r.Config.Id, errorMsg)
			continue
		}

		if err != nil {
			r.log.Errorf("unable to process result msg for read id '%s': %s", r.Config.Id, err)
			continue
		}

		r.log.Debug("processed result message for read id '%s'", r.Config.Id)
	}

	r.log.Debug("StartBackgroundReader exiting")
}

func (r *Read) processResultMessage(msg *types.ReadMessage) error {
	msgID := uuid.NewV4().String()

	record, err := r.Backend.ConvertReadToRecord(msgID, r.PlumberID, msg)
	if err != nil {
		return errors.Wrap(err, "unable to generate kafka payload")
	}

	// Send message payload to all attached streams
	r.AttachedClientsMutex.RLock()
	for id, s := range r.AttachedClients {
		r.log.Debugf("startBackgroundRead message to stream '%s'", id)
		s.MessageCh <- record
	}
	r.AttachedClientsMutex.RUnlock()

	return nil
}

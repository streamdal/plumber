package types

import (
	"context"
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/batchcorp/lucene2x/jsonquery"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/reader"
)

type AttachedStream struct {
	MessageCh chan *records.ReadRecord
}

type Read struct {
	AttachedClientsMutex *sync.RWMutex
	AttachedClients      map[string]*AttachedStream
	PlumberID            string
	ReadOptions          *opts.ReadOptions
	ContextCxl           context.Context
	CancelFunc           context.CancelFunc
	Backend              backends.Backend
	MsgDesc              *desc.MessageDescriptor
	Log                  *logrus.Entry
}

// StartRead is a goroutine that is launched when a read is started. It will continue running until plumber exits
// or a read is stopped via the API
func (r *Read) StartRead(ctx context.Context) {
	r.Log.Warn("Starting read")
	defer r.Backend.Close(ctx)

	r.ReadOptions.XActive = true

	resultsCh := make(chan *records.ReadRecord, 1)
	errorCh := make(chan *records.ErrorRecord, 1)

	go func() {
		// Notify the end user since this runs in a goroutine.
		// TODO: we should test read on CreateRead()
		if err := r.Backend.Read(ctx, r.ReadOptions, resultsCh, errorCh); err != nil {
			errorCh <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               err.Error(),
			}
		}
	}()

MAIN:
	for {

		select {
		case readRecord := <-resultsCh:
			// TODO: Implement a decoder pipeline -- decoding mid-flight without
			//   a pipeline will cause things to slow down starting at 100 msgs/s

			// Decode the msg
			decodedPayload, err := reader.Decode(r.ReadOptions, r.MsgDesc, readRecord.Payload)
			if err != nil {
				// TODO: need to send the err back to the client somehow
				r.Log.Errorf("unable to decode msg for backend '%s': %s", r.Backend.Name(), err)
				continue
			}

			// Message is JSON, perform query if needed
			filter := r.ReadOptions.GetFilter()
			if filter != nil && gjson.ValidBytes(decodedPayload) {
				found, err := jsonquery.Find(decodedPayload, filter.Query)
				if err != nil {
					r.Log.Errorf("Could not query payload, message ignored: %s", err)
					continue
				}
				if !found {
					r.Log.Debug("Message did not match filter, dropping")
					continue
				}
			}

			// Update read record with (maybe) decoded payload
			readRecord.Payload = decodedPayload

			// Send message payload to all attached streams
			r.AttachedClientsMutex.RLock()

			for id, s := range r.AttachedClients {
				r.Log.Debugf("Read message to stream '%s'", id)
				s.MessageCh <- readRecord
			}

			r.AttachedClientsMutex.RUnlock()
		case errRecord := <-errorCh:
			r.Log.Errorf("IMPORTANT: Received an error from reader: %+v", errRecord)
			// TODO: Send the error somehow to client
		case <-r.ContextCxl.Done():
			r.Log.Info("Read stopped")
			break MAIN
		}
	}

	r.Log.Debugf("reader id '%s' exiting", r.ReadOptions.XId)
}

package nats

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *Nats) Read(_ context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	n.log.Info("Listening for message(s) ...")

	var count int64

	// nats.Subscribe is async, use channel to wait to exit
	doneCh := make(chan struct{})
	defer close(doneCh)

	n.Client.Subscribe(readOpts.Nats.Args.Subject, func(msg *nats.Msg) {
		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
			}
			return
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Data,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_Nats{
				Nats: &records.Nats{
					Subject: msg.Subject,
					Value:   msg.Data,
				},
			},
		}

		if !readOpts.Continuous {
			doneCh <- struct{}{}
		}
	})

	<-doneCh

	return nil
}

func validateReadOptions(writeOpts *opts.ReadOptions) error {
	if writeOpts == nil {
		return errors.New("read options cannot be nil")
	}

	if writeOpts.Nats == nil {
		return errors.New("backend group options cannot be nil")
	}

	if writeOpts.Nats.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if writeOpts.Nats.Args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}

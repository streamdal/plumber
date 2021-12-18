package nats_jetstream

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *NatsJetstream) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	jsCtx, err := n.client.JetStream()
	if err != nil {
		return errors.Wrap(err, "failed to get jetstream context")
	}

	n.log.Info("Listening for message(s) ...")

	var count int64

	// nats.Subscribe is async, use channel to wait to exit
	doneCh := make(chan struct{})

	jsCtx.Subscribe(readOpts.NatsJetstream.Args.Stream, func(msg *nats.Msg) {
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
			Record: &records.ReadRecord_NatsJetstream{
				NatsJetstream: &records.NatsJetstream{
					Stream: readOpts.NatsJetstream.Args.Stream,
					Value:  msg.Data,
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

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.NatsJetstream == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.NatsJetstream.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Stream == "" {
		return ErrMissingStream
	}

	return nil
}

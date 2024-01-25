package rpubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (r *RedisPubsub) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	var count int64

	ps := r.client.Subscribe(ctx, readOpts.RedisPubsub.Args.Channels...)
	defer ps.Unsubscribe(ctx)

	r.log.Info("Listening for message(s) ...")

	doneCh := make(chan struct{})

	go func() {
		for {
			msg, err := ps.ReceiveMessage(ctx)
			if err != nil {
				util.WriteError(r.log, errorChan, fmt.Errorf("unable to receive redis pubsub messsage: %s", err))

				if !readOpts.Continuous {
					doneCh <- struct{}{}
					break
				}

				continue
			}

			count++

			serializedMsg, err := json.Marshal(msg)
			if err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
					Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
				}
				continue
			}

			count++

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            nil,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             []byte(msg.Payload),
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_RedisPubsub{
					RedisPubsub: &records.RedisPubsub{
						Value:     []byte(msg.Payload),
						Timestamp: time.Now().UTC().Unix(),
					},
				},
			}

			if !readOpts.Continuous {
				doneCh <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case <-doneCh:
		return nil
	}
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.RedisPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if readOpts.RedisPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(readOpts.RedisPubsub.Args.Channels) == 0 {
		return ErrMissingChannel
	}

	return nil
}

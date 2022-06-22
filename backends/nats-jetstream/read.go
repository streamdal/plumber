package nats_jetstream

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
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

	// nats.Subscribe is async, use channel to wait to exit
	doneCh := make(chan struct{})

	var count int64

	var consumerInfo *nats.ConsumerInfo
	var sub *nats.Subscription

	handler := func(msg *nats.Msg) {
		n.log.Debugf("Received new message on subject '%s'", msg.Subject)

		if err := msg.Ack(); err != nil {
			n.log.Warningf("unable to ack message")
		}

		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
			}
			return
		}

		jsRecord := &records.NatsJetstream{
			Stream: readOpts.NatsJetstream.Args.Stream,
			Value:  msg.Data,
		}

		if consumerInfo != nil {
			ci, err := msg.Sub.ConsumerInfo()
			if err != nil {
				n.log.Warningf("unable to fetch consumer info for msg: %s", err)
			} else {
				jsRecord.ConsumerName = ci.Name
				jsRecord.Sequence = int64(ci.Delivered.Stream)
			}
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Data,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_NatsJetstream{
				NatsJetstream: jsRecord,
			},
		}

		if !readOpts.Continuous {
			doneCh <- struct{}{}
		}
	}

	if readOpts.NatsJetstream.Args.CreateDurableConsumer || readOpts.NatsJetstream.Args.ExistingDurableConsumer {
		consumerInfo, err = n.createConsumer(jsCtx, readOpts.NatsJetstream.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create consumer")
		}

		sub, err = jsCtx.PullSubscribe(consumerInfo.Stream, consumerInfo.Name)
	} else {
		sub, err = jsCtx.Subscribe(readOpts.NatsJetstream.Args.Stream, handler)
	}

	if err != nil {
		n.log.Errorf("unable to subscribe: %s", err)
	}

	defer sub.Unsubscribe()
	defer n.cleanupConsumer(jsCtx, consumerInfo, readOpts.NatsJetstream.Args)

	if sub.Type() == nats.PullSubscription {
	TOP:
		for {
			msgs, err := sub.Fetch(1, nats.MaxWait(60*time.Second))
			if err != nil {
				if strings.Contains(err.Error(), "timeout") {
					continue
				}
			}

			for _, m := range msgs {
				handler(m)

				if !readOpts.Continuous {
					break TOP
				}
			}
		}
	}

	<-doneCh

	return nil
}

func (n *NatsJetstream) cleanupConsumer(jsCtx nats.JetStreamContext, ci *nats.ConsumerInfo, readArgs *args.NatsJetstreamReadArgs) {
	// Nothing to do if no consumer info or read args
	if ci == nil || readArgs == nil {
		return
	}

	// Nothing to do
	if !readArgs.CreateDurableConsumer && !readArgs.ExistingDurableConsumer {
		return
	}

	// Nothing to do if no jctx
	if jsCtx == nil {
		return
	}

	// Nothing to do if consumer should be kept
	if readArgs.KeepConsumer {
		return
	}

	// Consumer should be deleted
	if err := jsCtx.DeleteConsumer(ci.Stream, ci.Name); err != nil {
		n.log.Errorf("unable to delete consumer during cleanup: %s", err)
	}

	n.log.Debug("successfully deleted consumer '%s' during cleanup", ci.Name)
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

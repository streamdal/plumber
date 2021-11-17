package kubemq_queue

import (
	"context"
	"encoding/json"
	"time"

	queuesStream "github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (k *KubeMQ) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	k.log.Info("Listening for message(s) ...")

	var count int64

	for {
		response, err := k.client.Poll(context.Background(),
			queuesStream.NewPollRequest().
				SetChannel(readOpts.KubemqQueue.Args.QueueName).
				SetMaxItems(1). // TODO: flag?
				SetAutoAck(false).
				SetWaitTimeout(DefaultReadTimeout))
		if err != nil {
			return err
		}

		if !response.HasMessages() {
			continue
		}

		t := time.Now().UTC().Unix()

		for _, msg := range response.Messages {
			count++

			serializedMsg, err := json.Marshal(msg)
			if err != nil {
				return errors.Wrap(err, "unable to serialize message to JSON")
			}

			rec := &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            nil,
				ReceivedAtUnixTsUtc: t,
				Payload:             msg.Body,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Kubemq{
					Kubemq: &records.KubeMQ{
						Id:        msg.MessageID,
						ClientId:  msg.ClientID,
						Channel:   msg.Channel,
						Value:     msg.Body,
						Timestamp: 0,
						Sequence:  0,
					},
				},
			}

			if msg.Attributes != nil {
				rec.GetKubemq().Sequence = int64(msg.Attributes.Sequence)
				rec.GetKubemq().Timestamp = msg.Attributes.Timestamp
			}

			resultsChan <- rec
		}

		if err := response.AckAll(); err != nil {
			return errors.Wrap(err, "unable to acknowledge message(s)")
		}

		count++

		if !readOpts.Continuous {
			return nil
		}
	}

	return nil
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return errors.New("read options cannot be nil")
	}

	if readOpts.KubemqQueue == nil {
		return errors.New("backend group options cannot be nil")
	}

	if readOpts.KubemqQueue.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if readOpts.KubemqQueue.Args.QueueName == "" {
		return errors.New("queue name cannot be empty")
	}

	return nil
}

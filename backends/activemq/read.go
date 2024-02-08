package activemq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *ActiveMQ) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	var count int64

	sub, err := a.client.Subscribe(getDestinationRead(readOpts.Activemq.Args), stomp.AckClient)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

	a.log.Info("Listening for message(s) ...")

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-sub.C:
			if msg.Err != nil {
				errorChan <- &records.ErrorRecord{
					Error:               msg.Err.Error(),
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				}
				continue
			}

			count++

			// can marshal entire message: unsupported type: chan *stomp.Message
			serializedMsg, err := json.Marshal(msg.Body)
			if err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
					Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
				}
			}

			if err := a.client.Ack(msg); err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
					Error:               fmt.Sprintf("unable to ack message: %s", err),
				}
			}

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             msg.Body,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Activemq{
					Activemq: &records.ActiveMQ{
						Destination:    msg.Destination,
						ContentType:    msg.ContentType,
						SubscriptionId: msg.Subscription.Id(),
						Value:          msg.Body,
					},
				},
			}

			if !readOpts.Continuous {
				return nil
			}
		}
	}

	return nil
}

func getDestinationRead(args *args.ActiveMQReadArgs) string {
	if args.Topic != "" {
		return "/topic/" + args.Topic
	}
	return args.Queue
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.Activemq == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.Activemq.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrTopicOrQueue
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrTopicAndQueue
	}

	return nil
}

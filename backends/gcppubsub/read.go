package gcppubsub

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (g *GCPPubSub) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var count int64
	var m sync.Mutex

	// Standard way to cancel Receive in gcp's pubsub
	cctx, cancel := context.WithCancel(ctx)

	sub := g.client.Subscription(readOpts.GcpPubsub.Args.SubscriptionId)

	var readFunc = func(ctx context.Context, msg *pubsub.Message) {
		count++
		m.Lock()
		defer m.Unlock()

		if readOpts.GcpPubsub.Args.AckMessages {
			defer msg.Ack()
		}

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
			}
			return
		}

		var deliveryAttempt int32
		if msg.DeliveryAttempt != nil {
			deliveryAttempt = int32(*msg.DeliveryAttempt)
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Data,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_GcpPubsub{
				GcpPubsub: &records.GCPPubSub{
					Id:              msg.ID,
					Value:           msg.Data,
					Attributes:      msg.Attributes,
					PublishTime:     msg.PublishTime.UTC().Unix(),
					DeliveryAttempt: deliveryAttempt,
					OrderingKey:     msg.OrderingKey,
				},
			},
		}

		if !readOpts.Continuous {
			cancel()
			return
		}
	}

	g.log.Info("Listening for messages...")

	if err := sub.Receive(cctx, readFunc); err != nil {
		errorChan <- &records.ErrorRecord{
			Error:               err.Error(),
			OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
		}
	}

	return nil
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return errors.New("write options cannot be nil")
	}

	if readOpts.GcpPubsub == nil {
		return errors.New("backend group options cannot be nil")
	}

	if readOpts.GcpPubsub.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if readOpts.GcpPubsub.Args.SubscriptionId == "" {
		return errors.New("subscription ID cannot be empty")
	}

	return nil
}

package azure_servicebus

import (
	"context"
	"encoding/json"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureServiceBus) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	a.log.Info("Listening for message(s) ...")

	var count int64

	var handler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			return errors.Wrap(err, "unable to serialize message to JSON")
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Data,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_AzureServiceBus{
				AzureServiceBus: &records.AzureServiceBus{
					ContentType:      msg.ContentType,
					CorrelationId:    msg.CorrelationID,
					Value:            msg.Data,
					DeliveryCount:    msg.DeliveryCount,
					SessionId:        util.DerefString(msg.SessionID),
					GroupSequence:    util.DerefUint32(msg.GroupSequence),
					Id:               msg.ID,
					Label:            msg.Label,
					ReplyTo:          msg.ReplyTo,
					ReplyToGroupId:   msg.ReplyToGroupID,
					To:               msg.To,
					Ttl:              int64(msg.TTL.Seconds()),
					LockToken:        msg.LockToken.String(),
					SystemProperties: makeSystemProperties(msg.SystemProperties),
					UserProperties:   util.MapInterfaceToString(msg.UserProperties),
					Format:           msg.Format,
				},
			},
		}

		return msg.Complete(ctx)
	}

	if readOpts.AzureServiceBus.Args.Queue != "" {
		return a.readQueue(ctx, handler, readOpts)
	}

	if readOpts.AzureServiceBus.Args.Topic != "" {
		return a.readTopic(ctx, handler, readOpts)
	}

	return nil
}

func makeSystemProperties(p *servicebus.SystemProperties) *records.AzureSystemProperties {
	if p == nil {
		return &records.AzureSystemProperties{}
	}

	return &records.AzureSystemProperties{
		LockedUntil:            p.LockedUntil.Unix(),
		SequenceNumber:         util.DerefInt64(p.SequenceNumber),
		PartitionId:            int32(util.DerefInt16(p.PartitionID)),
		PartitionKey:           util.DerefString(p.PartitionKey),
		EnqueuedTime:           util.DerefTime(p.EnqueuedTime),
		DeadLetterSource:       util.DerefString(p.DeadLetterSource),
		ScheduledEnqueueTime:   util.DerefTime(p.ScheduledEnqueueTime),
		EnqueuedSequenceNumber: util.DerefInt64(p.EnqueuedSequenceNumber),
		ViaPartitionKey:        util.DerefString(p.ViaPartitionKey),
		Annotations:            util.MapInterfaceToString(p.Annotations),
	}
}

// readQueue reads messages from an ASB queue
func (a *AzureServiceBus) readQueue(ctx context.Context, handler servicebus.HandlerFunc, readOpts *opts.ReadOptions) error {
	queue, err := a.client.NewQueue(readOpts.AzureServiceBus.Args.Queue)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus queue client")
	}

	defer queue.Close(ctx)
	for {
		if err := queue.ReceiveOne(ctx, handler); err != nil {
			return err
		}
		if !readOpts.Continuous {
			return nil
		}
	}

	return nil
}

// readTopic reads messages from an ASB topic using the given subscription name
func (a *AzureServiceBus) readTopic(ctx context.Context, handler servicebus.HandlerFunc, readOpts *opts.ReadOptions) error {
	topic, err := a.client.NewTopic(readOpts.AzureServiceBus.Args.Topic)
	if err != nil {
		return errors.Wrap(err, "unable to create topic")
	}
	sub, err := topic.NewSubscription(readOpts.AzureServiceBus.Args.SubscriptionName)
	if err != nil {
		return errors.Wrap(err, "unable to create topic subscription")
	}

	defer sub.Close(ctx)

	for {
		if err := sub.ReceiveOne(ctx, handler); err != nil {
			return err
		}

		if !readOpts.Continuous {
			return nil
		}
	}

	return nil
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.AzureServiceBus == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.AzureServiceBus.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrQueueOrTopic
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrQueueAndTopic
	}

	return nil
}

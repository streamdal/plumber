package azure_servicebus

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureServiceBus) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, _ chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	a.log.Info("Listening for message(s) ...")

	if readOpts.AzureServiceBus.Args.Queue != "" {
		return a.readQueue(ctx, resultsChan, readOpts)
	}

	if readOpts.AzureServiceBus.Args.Topic != "" {
		return a.readTopic(ctx, resultsChan, readOpts)
	}

	return nil
}

func makeSystemProperties(p *azservicebus.ReceivedMessage) *records.AzureSystemProperties {
	if p == nil {
		return &records.AzureSystemProperties{}
	}

	return &records.AzureSystemProperties{
		LockedUntil:    p.LockedUntil.Unix(),
		SequenceNumber: util.DerefInt64(p.SequenceNumber),
		//PartitionId:            int32(util.DerefInt16(p.PartitionID)),
		PartitionKey:           util.DerefString(p.PartitionKey),
		EnqueuedTime:           util.DerefTime(p.EnqueuedTime),
		DeadLetterSource:       util.DerefString(p.DeadLetterSource),
		ScheduledEnqueueTime:   util.DerefTime(p.ScheduledEnqueueTime),
		EnqueuedSequenceNumber: util.DerefInt64(p.EnqueuedSequenceNumber),
		//ViaPartitionKey:        util.DerefString(p.ViaPartitionKey),
		//Annotations:            util.MapInterfaceToString(p.Annotations),
	}
}

// readQueue reads messages from an ASB queue
func (a *AzureServiceBus) readQueue(ctx context.Context, resultsChan chan *records.ReadRecord, readOpts *opts.ReadOptions) error {
	receiver, err := a.client.NewReceiverForQueue(readOpts.AzureServiceBus.Args.Queue, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus queue client")
	}

	defer func() {
		_ = receiver.Close(ctx)
	}()

	for {
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			return err
		}

		for i := range messages {
			if err = a.handleMessage(ctx, resultsChan, receiver, messages[i]); err != nil {
				return err
			}

			if !readOpts.Continuous {
				return nil
			}
		}
	}

	return nil
}

// readTopic reads messages from an ASB topic using the given subscription name
func (a *AzureServiceBus) readTopic(ctx context.Context, resultsChan chan *records.ReadRecord, readOpts *opts.ReadOptions) error {
	receiver, err := a.client.NewReceiverForSubscription(readOpts.AzureServiceBus.Args.Topic, readOpts.AzureServiceBus.Args.SubscriptionName, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus subscription client")
	}

	defer func() {
		_ = receiver.Close(ctx)
	}()

	for {
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			return err
		}

		for i := range messages {
			if err = a.handleMessage(ctx, resultsChan, receiver, messages[i]); err != nil {
				return err
			}

			if !readOpts.Continuous {
				return nil
			}
		}
	}

	return nil
}

func (a *AzureServiceBus) handleMessage(ctx context.Context, resultsChan chan *records.ReadRecord, receiver *azservicebus.Receiver, msg *azservicebus.ReceivedMessage) error {
	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "unable to serialize message to JSON")
	}

	resultsChan <- &records.ReadRecord{
		MessageId:           uuid.NewV4().String(),
		Num:                 util.DerefInt64(msg.SequenceNumber),
		ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
		Payload:             msg.Body,
		XRaw:                serializedMsg,
		Record: &records.ReadRecord_AzureServiceBus{
			AzureServiceBus: &records.AzureServiceBus{
				ContentType:   util.DerefString(msg.ContentType),
				CorrelationId: util.DerefString(msg.CorrelationID),
				Value:         msg.Body,
				DeliveryCount: msg.DeliveryCount,
				SessionId:     util.DerefString(msg.SessionID),
				//GroupSequence:    util.DerefUint32(msg.GroupSequence),
				Id: msg.MessageID,
				//Label:            msg.Label,
				ReplyTo: util.DerefString(msg.ReplyTo),
				//ReplyToGroupId:   msg.ReplyToGroupID,
				To:               util.DerefString(msg.To),
				Ttl:              int64(msg.TimeToLive.Seconds()),
				LockToken:        string(msg.LockToken[:]),
				SystemProperties: makeSystemProperties(msg),
				UserProperties:   util.MapInterfaceToString(msg.ApplicationProperties),
				//Format:           msg.Format,
			},
		},
	}

	return receiver.CompleteMessage(ctx, msg, nil)
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

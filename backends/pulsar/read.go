package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (p *Pulsar) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       readOpts.Pulsar.Args.Topic,
		SubscriptionName:            readOpts.Pulsar.Args.SubscriptionName,
		Type:                        getSubscriptionType(readOpts),
		SubscriptionInitialPosition: getSubscriptionInitialPosition(readOpts),
	})
	if err != nil {
		return errors.Wrap(err, "unable to create pulsar subscription")
	}

	defer consumer.Close()
	defer consumer.Unsubscribe()

	p.log.Info("Listening for message(s) ...")

	var count int64

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			if err == context.Canceled {
				p.log.Debug("context cancelled")
				return nil
			}

			util.WriteError(nil, errorChan, errors.Wrap(err, "unable to read pulsar message"))

			if !readOpts.Continuous {
				return nil
			}
			continue
		}

		consumer.Ack(msg)

		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
			}
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Payload(),
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_Pulsar{
				Pulsar: &records.Pulsar{
					Id:              fmt.Sprintf("%s", msg.ID()),
					Key:             msg.Key(),
					Topic:           msg.Topic(),
					Properties:      msg.Properties(),
					RedeliveryCount: msg.RedeliveryCount(),
					EventTime:       msg.EventTime().Format(time.RFC3339),
					IsReplicated:    msg.IsReplicated(),
					OrderingKey:     msg.OrderingKey(),
					ProducerName:    msg.ProducerName(),
					PublishTime:     msg.PublishTime().Format(time.RFC3339),
					Timestamp:       time.Now().UTC().Unix(),
					Value:           msg.Payload(),
				},
			},
		}

		if !readOpts.Continuous {
			return nil
		}
	}

	return nil
}

// getSubscriptionType converts string input of the subscription type to pulsar library's equivalent
func getSubscriptionType(readOpts *opts.ReadOptions) pulsar.SubscriptionType {
	switch readOpts.Pulsar.Args.SubscriptionType.String() {
	case "EXCLUSIVE":
		return pulsar.Exclusive
	case "FAILOVER":
		return pulsar.Failover
	case "KEYSHARED":
		return pulsar.KeyShared
	default:
		return pulsar.Shared
	}
}

func getSubscriptionInitialPosition(readOpts *opts.ReadOptions) pulsar.SubscriptionInitialPosition {
	switch readOpts.Pulsar.Args.InitialPosition.String() {
	case "PULSAR_EARLIEST":
		return pulsar.SubscriptionPositionEarliest
	default:
		return pulsar.SubscriptionPositionLatest
	}
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.Pulsar == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.Pulsar.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Topic == "" {
		return ErrEmptyTopic
	}

	if args.SubscriptionName == "" {
		return ErrEmptySubscriptionName
	}

	return nil
}

package nats_streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/stan.go"
	pb2 "github.com/nats-io/stan.go/pb"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (n *NatsStreaming) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	var count int64

	// stan.Subscribe is async, use channel to wait to exit
	doneCh := make(chan struct{}, 1)
	defer close(doneCh)

	var subFunc = func(msg *stan.Msg) {
		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			util.WriteError(n.log, errorChan, errors.Wrap(err, "unable to serialize message into JSON"))
		}

		metadata := map[string]string{
			"sequence_no":      fmt.Sprintf("%d", msg.Sequence),
			"crc32":            fmt.Sprintf("%d", msg.CRC32),
			"redelivered":      fmt.Sprintf("%t", msg.Redelivered),
			"redelivery_count": fmt.Sprintf("%d", msg.RedeliveryCount),
			"subject":          msg.Subject,
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			ReceivedAtUnixTsUtc: msg.Timestamp,
			Payload:             msg.Data,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_NatsStreaming{
				NatsStreaming: &records.NatsStreaming{
					Metadata:  metadata,
					Value:     msg.Data,
					Timestamp: msg.Timestamp,
				},
			},
		}

		// All read options except --last-received will default to follow mode, otherwise we will cause a panic here
		if !readOpts.Continuous && readOpts.NatsStreaming.Args.ReadLastAvailable {
			doneCh <- struct{}{}
		}
	}

	subOptions, err := n.getReadOptions(readOpts)
	if err != nil {
		return errors.Wrap(err, "unable to read from nats-streaming")
	}

	n.log.Info("Listening for messages...")

	sub, err := n.stanClient.Subscribe(readOpts.NatsStreaming.Args.Channel, subFunc, subOptions...)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return nil
	}

	return nil

}

// getReadOptions returns slice of options to pass to Stan.io. Only one read option of --last, --since, --seq, --new--only
// is allowed, so return early once we have a read option
func (n *NatsStreaming) getReadOptions(readOpts *opts.ReadOptions) ([]stan.SubscriptionOption, error) {
	opts := make([]stan.SubscriptionOption, 0)

	if readOpts.NatsStreaming.Args.DurableName != "" {
		opts = append(opts, stan.DurableName(readOpts.NatsStreaming.Args.DurableName))
	}

	if readOpts.NatsStreaming.Args.ReadAll {
		opts = append(opts, stan.DeliverAllAvailable())
		return opts, nil
	}

	if readOpts.NatsStreaming.Args.ReadLastAvailable {
		opts = append(opts, stan.StartWithLastReceived())
		return opts, nil
	}

	if readOpts.NatsStreaming.Args.ReadSince != "" {
		t, err := time.ParseDuration(readOpts.NatsStreaming.Args.ReadSince)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse --since option")
		}

		opts = append(opts, stan.StartAtTimeDelta(t))
		return opts, nil
	}

	// TODO: change proto type so we don't have to typecast
	if readOpts.NatsStreaming.Args.ReadSequenceNumber > 0 {
		opts = append(opts, stan.StartAtSequence(uint64(readOpts.NatsStreaming.Args.ReadSequenceNumber)))
		return opts, nil
	}

	// Default option is new-only
	opts = append(opts, stan.StartAt(pb2.StartPosition_NewOnly))

	return opts, nil
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.NatsStreaming == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.NatsStreaming.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Channel == "" {
		return ErrEmptyChannel
	}

	if args.ReadSequenceNumber > 0 {
		if args.ReadAll {
			return ErrInvalidReadOption
		}

		if args.ReadSince != "" {
			return ErrInvalidReadOption
		}

		if args.ReadLastAvailable {
			return ErrInvalidReadOption
		}
	}

	if args.ReadAll {
		if args.ReadSequenceNumber > 0 {
			return ErrInvalidReadOption
		}

		if args.ReadSince != "" {
			return ErrInvalidReadOption
		}

		if args.ReadLastAvailable {
			return ErrInvalidReadOption
		}
	}

	if args.ReadSince != "" {
		if args.ReadSequenceNumber > 0 {
			return ErrInvalidReadOption
		}

		if args.ReadAll {
			return ErrInvalidReadOption
		}

		if args.ReadLastAvailable {
			return ErrInvalidReadOption
		}
	}

	if args.ReadLastAvailable {
		if args.ReadSequenceNumber > 0 {
			return ErrInvalidReadOption
		}

		if args.ReadAll {
			return ErrInvalidReadOption
		}

		if args.ReadSince != "" {
			return ErrInvalidReadOption
		}
	}

	return nil
}

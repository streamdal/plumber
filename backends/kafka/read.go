package kafka

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
)

// DONE
func (k *Kafka) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(k.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	reader, err := NewReader(k.dialer, k.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	defer reader.Close()

	return k.read(ctx, reader, resultsChan, errorChan)
}

func (k *Kafka) read(ctx context.Context, reader *skafka.Reader, resultsChan chan *types.ReadMessage,
	errorChan chan *types.ErrorMessage) error {

	if reader == nil {
		return errors.New("reader cannot be nil")
	}

	k.log.Info("Initializing (could take a minute or two) ...")

	count := 1
	lastOffset := int64(-1)

	var lag *Lag

	if k.Options.Read.Lag {
		var err error

		lag, err = k.NewLag(ctx)
		if err != nil {
			return errors.Wrap(err, "unable to create new lag client")
		}
	}

	// init only one connection for partition discovery
	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := reader.ReadMessage(ctx)

		if err != nil {
			if !k.Options.Read.Follow {
				return errors.Wrap(err, "unable to read message (exiting)")
			}

			util.WriteError(k.log, errorChan, errors.Wrap(err, "unable to read message (continuing)"))
			continue
		}

		metadata := make(map[string]interface{}, 0)

		if k.Options.Read.Lag {
			lastOffset, err = lag.GetPartitionLastOffset(msg.Topic, msg.Partition)
			if err != nil {
				return errors.Wrap(err, "unable to obtain lastOffset for partition")
			}

			metadata["last_offset"] = lastOffset
		}

		resultsChan <- &types.ReadMessage{
			Value:      msg.Value,
			Metadata:   metadata,
			ReceivedAt: time.Now().UTC(),
			Num:        count,
			Raw:        msg,
		}

		if !k.Options.Read.Follow {
			break
		}

		count++
	}

	k.log.Debug("reader exiting")

	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.Kafka.ReadOffset < 0 {
		return errors.New("read offset must be >= 0")
	}

	return nil
}

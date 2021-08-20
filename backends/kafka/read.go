package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

const (
	RetrySampledReadInterval = 10 * time.Second
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

	var lag *Lag

	if k.Options.Read.Lag || k.Options.Read.Sampling.Enable {
		var err error

		lag, err = k.NewLag(ctx)
		if err != nil {
			return errors.Wrap(err, "unable to create new lag client")
		}
	}

	var err error
	var readType string

	if k.Options.Read.Sampling.Enable {
		readType = "sampled"
		err = k.performSampledRead(ctx, reader, lag, resultsChan, errorChan)
	} else {
		readType = "regular"
		err = k.performFullRead(ctx, reader, lag, resultsChan, errorChan)
	}

	if err != nil {
		return fmt.Errorf("unable to complete '%s' read: %s", readType, err)
	}

	k.log.Debug("reader exiting")

	return nil
}

// TODO: Implement
func (k *Kafka) performSampledRead(ctx context.Context, reader *skafka.Reader, lag *Lag,
	resultsChan chan *types.ReadMessage,
	errorChan chan *types.ErrorMessage,
) error {
	llog := k.log.WithField("method", "performSampledRead")

	topic := k.Options.Kafka.Topics[0]

	// TODO: This should probably be refreshed once in a while
	partitions, err := lag.discoverPartitions(topic)
	if err != nil {
		return fmt.Errorf("unable to discover partitions for topic '%s': %s", topic, err)
	}

	var count int
	var previousLatestOffset int64

	for {
		// Get the latest offset for topic
		currentLatestOffset, err := lag.GetLatestOffset(topic, partitions)
		if err != nil {
			util.WriteError(llog, errorChan, fmt.Errorf("unable to discover latest offset for topic '%s': %s",
				topic, err))

			if !k.Options.Read.Follow {
				break
			}

			llog.Warningf("unable to fetch currentLatestOffset for topic '%s', retrying in %s", topic, RetrySampledReadInterval)

			time.Sleep(RetrySampledReadInterval)

			continue
		}

		// Sleep for interval
		time.Sleep(k.Options.Read.Sampling.Interval)

		if previousLatestOffset == 0 {
			previousLatestOffset = currentLatestOffset
			continue
		}

		// previousOffset exists, determine the difference between previous and
		// latest offsets; apply the sample rate to the difference to get the
		// size of offset steps we'll need between every sample.
		step := int64((currentLatestOffset - previousLatestOffset) / k.Options.Read.Sampling.Rate)

		for offset := previousLatestOffset; offset < currentLatestOffset; offset += step {
			llog.Debugf("attempting to set next offset at '%d' for topic '%s'", offset, topic)

			// Set the offset at the correct position
			if err := reader.SetOffset(offset); err != nil {
				fullErr := fmt.Errorf("unable to set offset to '%d' for topic '%s': %s", offset, topic, err)

				util.WriteError(llog, errorChan, fullErr)

				if !k.Options.Read.Follow {
					return fullErr
				}

				time.Sleep(RetrySampledReadInterval)

				continue
			}

			// Attempt to read the msg at the specified offset
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				util.WriteError(llog, errorChan, fmt.Errorf("unable to set offset to '%d' for topic '%s': %s",
					offset, topic, err))

				time.Sleep(RetrySampledReadInterval)

				continue
			}

			count++

			resultsChan <- &types.ReadMessage{
				Value:      msg.Value,
				Raw:        msg,
				ReceivedAt: time.Now().UTC(),
				Num:        count,
			}
		}

		if !k.Options.Read.Follow {
			break
		}
	}

	llog.Debug("performSampledRead exiting")

	return nil
}

func (k *Kafka) performFullRead(ctx context.Context, reader *skafka.Reader, lag *Lag,
	resultsChan chan *types.ReadMessage,
	errorChan chan *types.ErrorMessage,
) error {
	llog := k.log.WithField("method", "performFullRead")

	count := 1

	// init only one connection for partition discovery
	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := reader.ReadMessage(ctx)

		if err != nil {
			if !k.Options.Read.Follow {
				return errors.Wrap(err, "unable to read message (exiting)")
			}

			util.WriteError(llog, errorChan, errors.Wrap(err, "unable to read message (continuing)"))
			continue
		}

		metadata := make(map[string]interface{}, 0)

		if k.Options.Read.Lag {
			lastOffset, err := lag.GetPartitionLastOffset(msg.Topic, msg.Partition)
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

	llog.Debug("performSampledRead exiting")

	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.Kafka.ReadOffset < 0 {
		return errors.New("read offset must be >= 0")
	}

	if opts.Read.Sampling.Enable {
		if opts.Kafka.UseConsumerGroup {
			return errors.New("sampling cannot be enabled while consumer group usage is enabled (choose one)")
		}

		if opts.Read.Sampling.Rate < 1 {
			return errors.New("sampling rate must be >0")
		}

		if opts.Read.Sampling.Interval > 5*time.Minute || opts.Read.Sampling.Interval < time.Second {
			return errors.New("sampling interval must be between 1s and 5 minutes")
		}
	}

	return nil
}

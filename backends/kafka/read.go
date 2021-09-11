package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	skafka "github.com/segmentio/kafka-go"
)

const (
	RetrySampledReadInterval = 10 * time.Second
)

func (k *Kafka) Read(
	ctx context.Context,
	readOpts *opts.ReadOptions,
	resultsChan chan *records.ReadRecord,
	errorChan chan *records.ErrorRecord,
) error {

	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read config")
	}

	reader, err := NewReaderForRead(k.dialer, k.connArgs, readOpts.Kafka.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	defer reader.Close()

	return k.read(ctx, readOpts, reader, resultsChan, errorChan)
}

func (k *Kafka) read(
	ctx context.Context,
	readOpts *opts.ReadOptions,
	reader *skafka.Reader,
	resultsChan chan *records.ReadRecord,
	errorChan chan *records.ErrorRecord,
) error {

	if reader == nil {
		return errors.New("reader cannot be nil")
	}

	k.log.Info("Initializing (could take a minute or two) ...")

	var lag *Lag

	// In addition to instantiating Lag when the user wants to include offset
	// info or view consumer lag, we also need to instantiate Lag to facilitate
	// sampled reads as they require offset lookup methods.
	if readOpts.Kafka.Args.IncludeOffsetInfo || readOpts.SampleOptions != nil || readOpts.Kafka.Args.Lag {
		var err error

		lag, err = k.NewLag(readOpts.Kafka.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create new lag client")
		}
	}

	var err error
	var readType string

	switch {
	case readOpts.Kafka.Args.Lag:
		readType = "lag"
		err = k.performLagRead(ctx, readOpts, lag, resultsChan, errorChan)
	case readOpts.SampleOptions != nil:
		readType = "sampled"
		err = k.performSampledRead(ctx, readOpts, reader, lag, resultsChan, errorChan)
	default:
		readType = "regular"
		err = k.performFullRead(ctx, readOpts, reader, lag, resultsChan, errorChan)
	}

	if err != nil {
		return fmt.Errorf("unable to complete '%s' read: %s", readType, err)
	}

	k.log.Debug("reader exiting")

	return nil
}

// TODO: Implement
func (k *Kafka) performLagRead(
	ctx context.Context,
	readOpts *opts.ReadOptions,
	lag *Lag,
	resultsChan chan *records.ReadRecord,
	errorChan chan *records.ErrorRecord,
) error {
	return nil
}

func (k *Kafka) performSampledRead(
	ctx context.Context,
	readOpts *opts.ReadOptions,
	reader *skafka.Reader,
	lag *Lag,
	resultsChan chan *records.ReadRecord,
	errorChan chan *records.ErrorRecord,
) error {

	llog := k.log.WithField("method", "performSampledRead")

	if lag == nil {
		return errors.New("lag cannot be nil with sampled reads")
	}

	topic := readOpts.Kafka.Args.Topics[0]

	// TODO: This should probably be refreshed once in a while
	partitions, err := lag.discoverPartitions(topic)
	if err != nil {
		return fmt.Errorf("unable to discover partitions for topic '%s': %s", topic, err)
	}

	var count int64
	var previousLatestOffset int64

	for {
		// Get the latest offset for topic
		currentLatestOffset, err := lag.GetAllPartitionsLastOffset(topic, partitions)
		if err != nil {
			util.WriteError(llog, errorChan, fmt.Errorf("unable to discover latest offset for topic '%s': %s",
				topic, err))

			if !readOpts.Continuous {
				break
			}

			llog.Warningf("unable to fetch currentLatestOffset for topic '%s', retrying in %s", topic, RetrySampledReadInterval)

			time.Sleep(RetrySampledReadInterval)

			continue
		}

		// Sleep for interval
		time.Sleep(util.DurationSec(readOpts.SampleOptions.SampleIntervalSeconds))

		if previousLatestOffset == 0 {
			previousLatestOffset = currentLatestOffset
			continue
		}

		// previousOffset exists, determine the difference between previous and
		// latest offsets; apply the sample rate to the difference to get the
		// size of offset steps we'll need between every sample.
		step := (currentLatestOffset - previousLatestOffset) / int64(readOpts.SampleOptions.SampleRate)

		for offset := previousLatestOffset; offset < currentLatestOffset; offset += step {
			llog.Debugf("attempting to set next offset at '%d' for topic '%s'", offset, topic)

			// Set the offset at the correct position
			if err := reader.SetOffset(offset); err != nil {
				fullErr := fmt.Errorf("unable to set offset to '%d' for topic '%s': %s", offset, topic, err)

				util.WriteError(llog, errorChan, fullErr)

				if !readOpts.Continuous {
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

			receivedAt := time.Now().UTC()

			metadata := make(map[string]string, 0)

			if readOpts.Kafka.Args.IncludeOffsetInfo {
				lastOffset, err := lag.GetPartitionLastOffset(msg.Topic, msg.Partition)
				if err != nil {
					return errors.Wrap(err, "unable to obtain lastOffset for partition")
				}

				metadata["last_offset"] = fmt.Sprint(lastOffset)
			}

			count++

			serializedMsg, err := json.Marshal(msg)
			if err != nil {
				return errors.Wrap(err, "unable to serialize kafka message into JSON")
			}

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            metadata,
				ReceivedAtUnixTsUtc: receivedAt.Unix(),
				Payload:             msg.Value,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Kafka{
					Kafka: &records.Kafka{
						Topic:     msg.Topic,
						Key:       msg.Key,
						Value:     msg.Value,
						Timestamp: msg.Time.Unix(),
						Offset:    msg.Offset,
						Partition: int32(msg.Partition),
						Headers:   convertKafkaHeadersToProto(msg.Headers),
					},
				},
			}
		}

		if !readOpts.Continuous {
			break
		}
	}

	llog.Debug("performSampledRead exiting")

	return nil
}

func (k *Kafka) performFullRead(
	ctx context.Context,
	readOpts *opts.ReadOptions,
	reader *skafka.Reader,
	lag *Lag,
	resultsChan chan *records.ReadRecord,
	errorChan chan *records.ErrorRecord,
) error {

	llog := k.log.WithField("method", "performFullRead")

	var count int64

	// init only one connection for partition discovery
	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if !readOpts.Continuous {
				return errors.Wrap(err, "unable to read message (exiting)")
			}

			util.WriteError(llog, errorChan, errors.Wrap(err, "unable to read message (continuing)"))
			continue
		}

		receivedAt := time.Now().UTC()
		metadata := make(map[string]string, 0)

		if readOpts.Kafka.Args.IncludeOffsetInfo && lag != nil {
			lastOffset, err := lag.GetPartitionLastOffset(msg.Topic, msg.Partition)
			if err != nil {
				return errors.Wrap(err, "unable to obtain lastOffset for partition")
			}

			metadata["last_offset"] = fmt.Sprint(lastOffset)
		}

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			return errors.Wrap(err, "unable to serialize kafka msg to JSON")
		}

		count++

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			Metadata:            metadata,
			ReceivedAtUnixTsUtc: receivedAt.Unix(),
			Payload:             msg.Value,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_Kafka{
				Kafka: &records.Kafka{
					Topic:     msg.Topic,
					Key:       msg.Key,
					Value:     msg.Value,
					Timestamp: msg.Time.Unix(),
					Offset:    msg.Offset,
					Partition: int32(msg.Partition),
					Headers:   convertKafkaHeadersToProto(msg.Headers),
				},
			},
		}

		if !readOpts.Continuous {
			break
		}
	}

	llog.Debug("performSampledRead exiting")

	return nil
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts.Kafka == nil {
		return errors.New("kafka read options cannot be nil")
	}

	if readOpts.Kafka.Args == nil {
		return errors.New("kafka read option args cannot be nil")
	}

	if readOpts.Kafka.Args.ReadOffset < 0 {
		return errors.New("read offset must be >= 0")
	}

	if readOpts.SampleOptions != nil {
		if readOpts.Kafka.Args.UseConsumerGroup {
			return errors.New("sampling cannot be enabled while consumer group usage is enabled")
		}

		if readOpts.SampleOptions.SampleRate < 1 {
			return errors.New("sampling rate must be >0")
		}

		samplingIntervalDuration := time.Duration(readOpts.SampleOptions.SampleIntervalSeconds) * time.Second

		if samplingIntervalDuration > 5*time.Minute || samplingIntervalDuration < time.Second {
			return errors.New("sampling interval must be between 1s and 5 minutes")
		}
	}

	return nil
}

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"

	"github.com/streamdal/plumber/types"
)

const (
	DefaultLagInterval = 5 * time.Second
)

type Lag struct {
	dialer   *skafka.Dialer
	conns    map[string]*skafka.Conn // Conns for each topic
	readArgs *args.KafkaReadArgs
	connArgs *args.KafkaConn
	log      *logrus.Entry
	ctx      context.Context
}

// NewLag creates a lag instance but does not perform any lag operations
func (k *Kafka) NewLag(readArgs *args.KafkaReadArgs) (*Lag, error) {
	if err := validateReadArgsForLag(readArgs); err != nil {
		return nil, errors.Wrap(err, "unable to validate read options for lag")
	}

	conns, err := ConnectAllTopics(k.dialer, k.connArgs, readArgs.Topics)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create initial connections")
	}

	return &Lag{
		dialer:   k.dialer,
		conns:    conns,
		connArgs: k.connArgs,
		readArgs: readArgs,
		log:      logrus.WithField("pkg", "kafka/lag"),
	}, nil
}

func validateReadArgs(readArgs *args.KafkaReadArgs) error {
	if readArgs == nil {
		return errors.New("read args cannot be nil")
	}

	if len(readArgs.Topics) == 0 {
		return errors.New("at least one topic must be set")
	}

	return nil
}

func validateReadArgsForLag(readArgs *args.KafkaReadArgs) error {
	if err := validateReadArgs(readArgs); err != nil {
		return errors.Wrap(err, "unable to complete base read args validation")
	}

	if readArgs.Lag && readArgs.LagConsumerGroup == "" {
		return errors.New("must include lag consumer group if --lag is specified")
	}

	return nil
}

// Lag fetches topic stats on the given interval and returns them over the
// resultsChan. This is a blocking call.
func (l *Lag) Lag(
	ctx context.Context,
	resultsCh chan *records.ReadRecord,
	errorCh chan<- *records.ErrorRecord,
	interval time.Duration,
) error {
	t := time.NewTicker(interval)

	// This metadata is used by kafka's DisplayMessage to determine that the
	// record contains lag info instead of a regular payload.
	metadata := map[string]string{
		"lag": "true",
	}

MAIN:
	for {
		select {
		case <-ctx.Done():
			l.log.Warn("context cancelled")
			break MAIN
		case <-t.C:
			topicStats, err := l.getConsumerGroupLag(ctx)
			if err != nil {
				util.WriteError(l.log, errorCh, errors.Wrap(err, "unable to get consumer lag"))

				continue MAIN
			}

			topicStatsJSON, err := json.Marshal(topicStats)
			if err != nil {
				util.WriteError(l.log, errorCh, errors.Wrap(err, "unable to marshal topic stats to JSON"))

				continue MAIN
			}

			resultsCh <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Metadata:            metadata,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				XRaw:                topicStatsJSON,
			}
		}
	}

	l.log.Debug("exiting")

	return nil
}

// GetPartitionLastOffset - gets the last offset for a given partition. It is
// used by both Read() and Lag() to display offset stats and/or calculate lag.
func (l *Lag) GetPartitionLastOffset(topic string, part int) (int64, error) {
	partitions, err := l.discoverPartitions(topic)
	if err != nil {
		return -1, errors.Wrapf(err, "unable to discover partitions")
	}

	var partConn *skafka.Conn

	for _, pt := range partitions {
		if pt.ID == part {
			partConn, err = connect(l.dialer, l.connArgs, topic, pt.ID)
			if err != nil {
				return -1, errors.Wrap(err, "unable establish a connection to the partition")
			}

			break
		}
	}

	if partConn == nil {
		return 0, fmt.Errorf("partition not found for topic '%s'", topic)
	}

	defer partConn.Close()

	_, lastOffset, err := partConn.ReadOffsets()
	if err != nil {
		return -1, fmt.Errorf("unable to read offsets for partition '%d': %s", part, err)
	}

	return lastOffset, nil
}

// GetAllPartitionsLastOffset finds the MAX lastOffset across given partitions
// and topic.
func (l *Lag) GetAllPartitionsLastOffset(topic string, partitions []skafka.Partition) (int64, error) {
	lastOffsets := make([]int64, 0)

	for _, v := range partitions {
		conn, err := connect(l.dialer, l.connArgs, topic, v.ID)
		if err != nil {
			return 0, fmt.Errorf("unable to create leader connection for topic '%s', partition ID '%d': %s",
				topic, v.ID, err)
		}

		_, lastOffset, err := conn.ReadOffsets()
		if err != nil {
			return 0, fmt.Errorf("unable to read offset for topic '%s', partition ID '%d': %s",
				topic, v.ID, err)
		}

		lastOffsets = append(lastOffsets, lastOffset)
	}

	var maxOffset int64

	for _, offset := range lastOffsets {
		if offset > maxOffset {
			maxOffset = offset
		}
	}

	return maxOffset, nil
}

func (l *Lag) getConsumerGroupLag(ctx context.Context) ([]*types.TopicStats, error) {
	topicPartitionMap := make(map[string][]skafka.Partition)

	// Build partition list for _all_ topics
	for _, topic := range l.readArgs.Topics {
		partitionList, err := l.discoverPartitions(topic)
		if err != nil {
			return nil, fmt.Errorf("unable to discover partition list for topic '%s': %s", topic, err)
		}

		topicPartitionMap[topic] = partitionList
	}

	topicStats := make([]*types.TopicStats, 0)

	for tpKey, partValue := range topicPartitionMap {
		partitionStats := make(map[int]*types.PartitionStats, 0)

		for _, part := range partValue {
			lagPerPartition, err := l.getPartitionLag(ctx, tpKey, l.readArgs.LagConsumerGroup, part.ID)
			if err != nil {
				return nil, fmt.Errorf("unable to get per-partition lag for topic '%s', partition '%d': %s",
					tpKey, part.ID, err)
			}

			partitionStats[part.ID] = &types.PartitionStats{
				MessagesBehind: lagPerPartition,
			}
		}

		topicStats = append(topicStats, &types.TopicStats{
			TopicName:  tpKey,
			GroupID:    l.readArgs.LagConsumerGroup,
			Partitions: partitionStats,
		})
	}

	return topicStats, nil
}

// discoverPartitions returns a list of partitions for a specific topic. It is
// used by Read() to display offset stats.
func (l *Lag) discoverPartitions(topic string) ([]skafka.Partition, error) {
	conn, ok := l.conns[topic]
	if !ok {
		return nil, fmt.Errorf("unable to lookup connection for topic '%s'", topic)
	}

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get partition list")
	}

	return partitions, nil
}

// getPartitionLag fetches the lag for a given topic and partition.
func (l *Lag) getPartitionLag(ctx context.Context, topic string, groupId string, part int) (int64, error) {
	if _, ok := l.conns[topic]; !ok {
		return -1, fmt.Errorf("topic '%s' does not exist in conn map", topic)
	}

	remoteAddr := l.conns[topic].RemoteAddr()

	// get last offset per partition
	lastOffset, err := l.GetPartitionLastOffset(topic, part)
	if err != nil {
		return -1, errors.Wrap(err, "Unable to get last partion offset")
	}
	// obtain last committed offset for a given partition and consumer group
	kafkaClient := &skafka.Client{Addr: remoteAddr}

	offsetResponse, err := kafkaClient.OffsetFetch(ctx, &skafka.OffsetFetchRequest{
		Addr:    remoteAddr,
		GroupID: groupId,
		Topics:  map[string][]int{topic: {part}},
	})

	if err != nil {
		return -1, errors.Wrap(err, "Unable to obtain last committed offset per partition")
	}

	var lastCommittedOffset int64

	for _, v := range offsetResponse.Topics[topic] {
		if v.Partition == part {
			lastCommittedOffset = v.CommittedOffset
			break
		}
	}

	lagInPartition := lastOffset - lastCommittedOffset

	return lagInPartition, nil
}

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Lag struct {
	dialer  *skafka.Dialer
	conns   map[string]*skafka.Conn
	options *options.Options
	log     *logrus.Entry
}

func (k *Kafka) NewLag(ctx context.Context) (*Lag, error) {
	conns, err := ConnectAllTopics(k.dialer, k.Options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create initial connections")
	}

	return &Lag{
		dialer:  k.dialer,
		conns:   conns,
		options: k.Options,
		log:     logrus.WithField("pkg", "kafka/lag"),
	}, nil
}

// Lag fetches topic stats on the given interval and returns them over the
// resultsChan. This is a blocking call.
func (l *Lag) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	if err := validateLagOptions(l.options, resultsCh, interval); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	t := time.NewTicker(interval)

MAIN:
	for {
		select {
		case <-ctx.Done():
			l.log.Debug("context cancelled")
			break MAIN
		case <-t.C:
			topicStats, err := l.getConsumerGroupLag(ctx)
			if err != nil {
				return errors.Wrap(err, "unable to fetch lag stats")
			}

			resultsCh <- topicStats
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
			partConn, err = connect(l.dialer, l.options, topic, pt.ID)
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

func (l *Lag) getConsumerGroupLag(ctx context.Context) ([]*types.TopicStats, error) {
	topicPartitionMap := make(map[string][]skafka.Partition)

	// Build partition list for _all_ topics
	for _, topic := range l.options.Kafka.Topics {
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
			lagPerPartition, err := l.getPartitionLag(ctx, tpKey, l.options.Kafka.GroupID, part.ID)
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
			GroupID:    l.options.Kafka.GroupID,
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

func validateLagOptions(opts *options.Options, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	if len(opts.Kafka.Brokers) == 0 {
		return errors.New("no broker address available")

	}

	if len(opts.Kafka.Topics) == 0 {
		return errors.New("no topic available in options")
	}

	if resultsCh == nil {
		return errors.New("results channel cannot be nil")
	}

	if interval == 0 {
		return errors.New("interval cannot be 0")
	}

	return nil
}

package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
)

type Lag struct {
	dialer  *skafka.Dialer
	conns   map[string]*skafka.Conn
	options *options.Options
}

func NewLag(opts *options.Options, dialer *skafka.Dialer) (*Lag, error) {
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if dialer == nil {
		return nil, errors.New("dialer cannot be nil")
	}

	conns, err := ConnectAllTopics(dialer, opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create initial connections")
	}

	return &Lag{
		dialer:  dialer,
		conns:   conns,
		options: opts,
	}, nil
}

// validate cli options and init connection
func (k *Kafka) Lag(ctx context.Context) ([]*types.Lag, error) {
	if err := validateLagOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	kafkaLag, err := NewKafkaLagConnection(opts)

	if err != nil {
		return errors.Wrap(err, "unable to create connection")
	}

	for _, v := range kafkaLag.partitionDiscoverConn {
		defer v.Close()
	}

	groupId := opts.Kafka.GroupID

	return kafkaLag.LagCalculationForConsumerGroup(groupId, opts)
}

// calculate lag with a given connection
func (kLag *Lagger) LagCalculationForConsumerGroup(groupId string, opts *options.Options) error {

	topicPartionMap := make(map[string][]skafka.Partition)

	for tp, kc := range kLag.partitionDiscoverConn {

		partitionList, err := kc.ReadPartitions(tp)

		if err != nil {
			return errors.Wrap(err, "unable to obtain partitions")
		}

		topicPartionMap[tp] = partitionList
	}

	// calculate and print lag
	var sb strings.Builder

	for tpKey, partValue := range topicPartionMap {

		for _, part := range partValue {

			lagPerPartition, err := kLag.LagCalculationPerPartition(tpKey, groupId, part.ID, opts)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("unable to calculate lag for partition %v", part))
			}

			sb.WriteString(fmt.Sprintf("Lag in partition %v is <%v messages> for consumer with group-id \"%s\" \n", part.ID, lagPerPartition, groupId))
		}

		printer.Print(sb.String())

	}

	return nil

}

func (l *Lag) discoverPartitions(topic string) ([]skafka.Partition, error) {
	conn, ok := l.Conns[topic]
	if !ok {
		return nil, fmt.Errorf("unable to lookup connection for topic '%s'", topic)
	}

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get partition list")
	}

	return partitions, nil
}

// DONE
func (l *Lag) GetPartitionLastOffset(topic string, groupId string, part int) (int64, error) {
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

	_, lastOffet, err := partConn.ReadOffsets()
	if err != nil {
		return -1, fmt.Errorf("unable to read offsets for partition '%d': %s", part, err)
	}

	return lastOffet, nil
}

// create new connection per topic and address
func (kLag *Lagger) LagCalculationPerPartition(topic string, groupId string, part int, opts *options.Options) (int64, error) {

	// get last offset per partition

	lastOffset, err := kLag.GetLastOffsetPerPartition(topic, groupId, part, opts)

	// obtain last commited offset for a given partition and consumer group

	kcli := &skafka.Client{Addr: kLag.partitionDiscoverConn[topic].RemoteAddr()}

	offsetResponse, err := kcli.OffsetFetch(context.Background(), &skafka.OffsetFetchRequest{
		Addr:    kLag.partitionDiscoverConn[topic].RemoteAddr(),
		GroupID: groupId,
		Topics:  map[string][]int{topic: {part}},
	})

	if err != nil {
		return -1, errors.Wrap(err, "Unable to obtain last commited offset per partition")
	}

	var lastCommitedOffset int64

	for _, v := range offsetResponse.Topics[topic] {
		if v.Partition == part {
			lastCommitedOffset = v.CommittedOffset
			break
		}
	}

	lagInPartition := lastOffset - lastCommitedOffset

	return lagInPartition, nil

}

func validateLagOptions(opts *options.Options) error {

	if len(opts.Kafka.Brokers) == 0 {
		return errors.New("no broker address available")

	}

	if len(opts.Kafka.Topics) == 0 {
		return errors.New("no topic available in options")

	}

	return nil
}

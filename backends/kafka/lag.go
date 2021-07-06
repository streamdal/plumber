package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
)

// validate cli options and init connection
func Lag(opts *cli.Options) error {
	if err := validateLagOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	kafkaLag, err := NewKafkaLagConnection(opts)

	for _, v := range kafkaLag.partitionDiscoverConn {
		defer v.Close()
	}

	if err != nil {
		return errors.Wrap(err, "unable to create connection")
	}

	groupId := opts.Kafka.GroupID

	return kafkaLag.LagCalculationForConsumerGroup(groupId, opts)
}

// calculate lag with a given connection
func (kLag *KafkaLag) LagCalculationForConsumerGroup(groupId string, opts *cli.Options) error {

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

func discoverPartitions(topic string, partDiscoverConn *skafka.Conn, opts *cli.Options) ([]skafka.Partition, error) {

	partitions, err := partDiscoverConn.ReadPartitions(topic)

	if err != nil {
		return nil, errors.Wrap(err, "Failed to get partition list")
	}

	return partitions, nil
}

func (kLag *KafkaLag) GetLastOffsetPerPartition(topic string, groupId string, part int, opts *cli.Options) (int64, error) {


	partitions, err := discoverPartitions(topic, kLag.partitionDiscoverConn[topic], opts)

	if err != nil {
		return -1, err
	}

	var partConn *skafka.Conn

	for _, pt := range partitions {
		if pt.ID == part {
			partConn, err = newConnectionPerPartition(topic, pt.ID, opts)

			defer partConn.Close()

			if err != nil {
				return -1, errors.Wrap(err, "Unable establish a connection to the partition")
			}
		}
	}

	_, lastOffet, err := partConn.ReadOffsets()

	return lastOffet, err
}

// create new connection per topic and address
func (kLag *KafkaLag) LagCalculationPerPartition(topic string, groupId string, part int, opts *cli.Options) (int64, error) {

	// get last offset per partition

	lastOffset, err := kLag.GetLastOfssetPerPartition(topic, groupId, part, opts)

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

func validateLagOptions(opts *cli.Options) error {

	if len(opts.Kafka.Brokers) == 0 {
		return errors.New("no broker address available")

	}

	if len(opts.Kafka.Topics) == 0 {
		return errors.New("no topic available in options")

	}

	return nil
}

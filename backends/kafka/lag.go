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

	kafkaConn, err := NewConnection(opts)

	if err != nil {
		return errors.Wrap(err, "unable to create connection")
	}

	kafkaConn.RemoteAddr()

	groupId := opts.Kafka.GroupID

	defer kafkaConn.Close()

	return LagCalculation(kafkaConn, opts.Kafka.Topic, groupId, opts)
}

// calculate lag with a given connection
func LagCalculation(kc *skafka.Conn, topic string, groupId string, opts *cli.Options) error {

	partitionList, err := kc.ReadPartitions(topic)

	if err != nil {
		return errors.Wrap(err, "unable to obtain partitions")
	}

	// calculate and print lag
	var sb strings.Builder

	for _, part := range partitionList {

		lagPerPartition, err := LagCalculationPerPartition(kc, topic, groupId, part.ID, opts)

		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("unable to calculate lag for partition %v", part))
		}

		sb.WriteString(fmt.Sprintf("Lag for partition %v is %v \n", part.ID, lagPerPartition))
	}

	printer.Print(sb.String())

	return nil

}

func LagCalculationPerPartition(kc *skafka.Conn, topic string, groupId string, part int, opts *cli.Options) (int64, error) {

	// get last offset in partition

	partitions, err := kc.ReadPartitions(topic)

	var newConn *skafka.Conn

	for _, v := range partitions {
		if v.ID == part {
			newConn, err = NewConnection(opts)

			defer newConn.Close()
		}
	}

	_, lastOffet, err := newConn.ReadOffsets()

	// obtain last commited offset for a given partition

	kcli := &skafka.Client{Addr: kc.RemoteAddr()}

	offsetResponse, err := kcli.OffsetFetch(context.Background(), &skafka.OffsetFetchRequest{
		Addr:    newConn.RemoteAddr(),
		GroupID: groupId,
		Topics:  map[string][]int{topic: {part}},
	})

	if err != nil {
		return -1, errors.Wrap(err, "unable to obtain last commited offset per partition")
	}

	var lastCommitedOffset int64

	for _, v := range offsetResponse.Topics[topic] {
		if v.Partition == part {
			lastCommitedOffset = v.CommittedOffset
			break
		}
	}

	lagInPartition := lastOffet - lastCommitedOffset

	return lagInPartition, nil

}

func validateLagOptions(opts *cli.Options) error {

	if opts.Kafka.Address == "" {
		return fmt.Errorf("No broker address available")
	}

	if opts.Kafka.Topic == "" {
		return fmt.Errorf("No topic available in options")
	}

	return nil
}

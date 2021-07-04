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

		sb.WriteString(fmt.Sprintf("Lag for partition %v is %T \n", part, lagPerPartition))
	}

	printer.Print(sb.String())

	return nil

}

func LagCalculationPerPartition(kc *skafka.Conn, topic string, groupId string, part int, opts *cli.Options) (int64, error) {

	tempPartitions, err := kc.ReadPartitions(topic)

	var newConn *skafka.Conn

	for _, v := range tempPartitions {
		if v.ID == part {
			newConn, err = NewConnection(opts)

			defer newConn.Close()
		}
	}

	_, lOff, err := newConn.ReadOffsets()

	kcli := &skafka.Client{Addr: kc.RemoteAddr()}

	// obtain last commited offset for a given partition

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

	lagInPartition := lOff - lastCommitedOffset

	return lagInPartition, nil

}

func validateLagOptions(opts *cli.Options) error {
	if !opts.ReadLag {
		return fmt.Errorf("Read Lag option isn't available: %t", opts.ReadLag)
	}

	if opts.Kafka.Topic == "" {
		return fmt.Errorf("No topic available in options")
	}

	return nil
}

package kafka

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/cli"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
)

// validate cli options and init connection
func Lag(opts *cli.Options) error {
	if err := validateLagOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	kafkaConn, kafkaClient, err := NewConnection(opts)

	if err != nil {
		return errors.Wrap(err, "unable to create connection")
	}

	kafkaConn.RemoteAddr()

	groupId := opts.Kafka.GroupID

	defer kafkaConn.Close()

	return LagCalculation(kafkaConn, opts.Kafka.Topic, kafkaClient, groupId)
}

// calculate lag with a given connection
func LagCalculation(kc *skafka.Conn, topic string, kcli *skafka.Client, groupId string) error {

	partitionList, err := kc.ReadPartitions(topic)

	if err != nil {
		return errors.Wrap(err, "unable to obtain partitions")
	}

	// goal: get lag per partition in topic

	offsetResponse, err := kcli.OffsetFetch(context.Background(), &skafka.OffsetFetchRequest{
		Addr:    kc.RemoteAddr(),
		GroupID: groupId,
		Topics:  map[string][]int{topic: pa},
	})

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

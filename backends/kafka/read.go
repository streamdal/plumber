package kafka

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/kafka/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

// Read is the entry point function for performing read operations in Kafka.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	kafkaReader, err := NewReader(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	defer kafkaReader.Conn.Close()
	defer kafkaReader.Reader.Close()

	k := &Kafka{
		Options: opts,
		msgDesc: md,
		reader:  kafkaReader.Reader,
		log:     logrus.WithField("pkg", "kafka/read.go"),
	}

	return k.Read()
}

// Read will attempt to consume one or more messages from a given topic,
// optionally decode it and/or convert the returned output.
//
// This method SHOULD be able to recover from network hiccups.
func (k *Kafka) Read() error {
	k.log.Info("Initializing (could take a minute or two) ...")

	count := 1
	lastOffset := int64(-1)
	lastPartitionProcessed := -1

	var lagConn *Lagger

	// init only one connection for partition discovery
	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := k.reader.ReadMessage(context.Background())

		if err != nil {
			if !k.Options.ReadFollow {
				return errors.Wrap(err, "unable to read message")
			}

			printer.Error(fmt.Sprintf("Unable to read message: %s", err))
			continue
		}

		data, err := reader.Decode(k.Options, k.msgDesc, msg.Value)

		if err != nil {
			return err
		}

		if k.Options.ReadLag && msg.Partition != lastPartitionProcessed {

			lastPartitionProcessed = msg.Partition

			lagConn, err = NewKafkaLagConnection(k.Options)

			if err != nil {
				k.log.Debugf("Unable to connect establish a kafka lag connection: %s", err)
				continue
			}

			lastOffset, err = lagConn.GetLastOffsetPerPartition(msg.Topic, k.reader.Config().GroupID, msg.Partition, k.Options)

			if err != nil {
				return errors.Wrap(err, "unable to obtain lastOffset for partition")
			}
		}

		offsetInfo := &types.OffsetInfo{
			Count:      count,
			LastOffset: lastOffset,
		}

		printer.PrintKafkaResult(k.Options, offsetInfo, msg, data)

		if !k.Options.ReadFollow {
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

package kafka

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

// Read is the entry point function for performing read operations in Kafka.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options, md *desc.MessageDescriptor) error {
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
		MsgDesc: md,
		Reader:  kafkaReader.Reader,
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

	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := k.Reader.ReadMessage(context.Background())
		if err != nil {
			if !k.Options.ReadFollow {
				return errors.Wrap(err, "unable to read message")
			}

			printer.Error(fmt.Sprintf("Unable to read message: %s", err))
			continue
		}

		data, err := reader.Decode(k.Options, k.MsgDesc, msg.Value)
		if err != nil {
			return err
		}

		printer.PrintKafkaResult(k.Options, count, msg, data)

		if !k.Options.ReadFollow {
			break
		}

		count++
	}

	k.log.Debug("Reader exiting")

	return nil
}

func validateReadOptions(opts *cli.Options) error {
	if opts.Kafka.ReadOffset < 0 {
		return errors.New("read offset must be >= 0")
	}

	return nil
}

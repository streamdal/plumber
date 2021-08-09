package kubemq_queue

import (
	"context"

	"github.com/jhump/protoreflect/desc"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"
)

func Write(opts *cli.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	k := &KubeMQQueue{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "kubemq-queue/write.go"),
	}

	return k.Write(writeValues)
}
func (k *KubeMQQueue) Write(writeValues [][]byte) error {
	defer func() {
		_ = k.Client.Close()
	}()

	var batch []*queues_stream.QueueMessage
	for i := 0; i < len(writeValues); i++ {
		batch = append(batch, queues_stream.NewQueueMessage().
			SetChannel(k.Options.KubeMQQueue.Queue).SetBody(writeValues[i]))
	}
	results, err := k.Client.Send(context.Background(), batch...)
	if err != nil {
		return err
	}
	for i := 0; i < len(results.Results); i++ {
		if results.Results[i].IsError {
			k.log.Errorf("Failed wrote message %d to queue %s, error %s", i+1, k.Options.KubeMQQueue.Queue, results.Results[i].Error)
		} else {
			k.log.Infof("Successfully wrote message %d to queue %s", i+1, k.Options.KubeMQQueue.Queue)
		}
	}

	return nil
}

// validateWriteOptions ensures the correct CLI options are specified for write
func validateWriteOptions(opts *cli.Options) error {
	if opts.KubeMQQueue.Queue == "" {
		return errMissingQueue
	}
	return nil
}

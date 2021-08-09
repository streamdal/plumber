package rabbitmqStreams

import (
	"sync"
	"sync/atomic"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

func Write(opts *options.Options, md *desc.MessageDescriptor) error {

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return err
	}

	defer client.Close()

	producer, err := client.NewProducer(opts.RabbitMQStreams.Stream,
		stream.NewProducerOptions().
			SetProducerName(opts.RabbitMQStreams.ClientName).
			SetBatchSize(len(writeValues)).
			SetBatchSize(len(writeValues)))
	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq streams producer")
	}

	defer producer.Close()

	r := &RabbitMQStreams{
		Client:    client,
		Producer:  producer,
		Options:   opts,
		waitGroup: &sync.WaitGroup{},
		log:       logrus.WithField("pkg", "rabbitmq-streams/write.go"),
	}

	chPublishConfirm := producer.NotifyPublishConfirmation()
	chPublishError := producer.NotifyPublishError()
	go r.handleConfirm(chPublishConfirm)
	go r.handleError(chPublishError)

	for _, value := range writeValues {
		if err := r.Write(value); err != nil {
			r.log.Error(err)
			continue
		}
		r.waitGroup.Add(1)
	}

	r.waitGroup.Wait()

	return nil
}

func (r *RabbitMQStreams) handleError(publishError stream.ChannelPublishError) {
	var totalMessages int32
	for {
		pError := <-publishError
		atomic.AddInt32(&totalMessages, 1)
		var data [][]byte
		if pError.UnConfirmedMessage != nil {
			data = pError.UnConfirmedMessage.Message.GetData()
		}
		r.log.Errorf("Failed to publish message: %s ,  error: %s. Total %d  \n", data, pError.Err, totalMessages)
	}
}

func (r *RabbitMQStreams) handleConfirm(confirms stream.ChannelPublishConfirm) {
	for confirmed := range confirms {
		for _, msg := range confirmed {
			if msg.Confirmed {
				r.log.Infof("Published message to stream '%s': %s", r.Options.RabbitMQStreams.Stream, msg.Message.GetData())
			} else {
				r.log.Errorf("Message failed: %s", msg.Message.GetData())
			}
			r.waitGroup.Done()
		}
	}
}

func (r *RabbitMQStreams) Write(value []byte) error {
	if err := r.Producer.Send(amqp.NewMessage(value)); err != nil {
		return errors.Wrapf(err, "unable to publish message to stream '%s'", r.Options.RabbitMQStreams.Stream)
	}

	return nil
}

package rabbitmq_streams

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func (r *RabbitMQStreams) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	client, err := newClient(r.Options)
	if err != nil {
		return err
	}

	defer client.Close()

	producer, err := client.NewProducer(r.Options.RabbitMQStreams.Stream,
		stream.NewProducerOptions().
			SetProducerName(r.Options.RabbitMQStreams.ClientName).
			SetBatchSize(len(messages)).
			SetBatchSize(len(messages)))
	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq streams producer")
	}

	defer producer.Close()

	wg := &sync.WaitGroup{}

	chPublishConfirm := producer.NotifyPublishConfirmation()
	chPublishError := producer.NotifyPublishError()

	go r.handleConfirm(chPublishConfirm, wg)
	go r.handleError(chPublishError, errorCh)

	for _, msg := range messages {
		if err := r.write(producer, msg.Value); err != nil {
			util.WriteError(r.log, errorCh, err)
			continue
		}
		wg.Add(1)
	}

	wg.Wait()

	return nil
}

func (r *RabbitMQStreams) handleError(publishError stream.ChannelPublishError, errorCh chan *types.ErrorMessage) {
	var totalMessages int32
	for {
		pError := <-publishError
		atomic.AddInt32(&totalMessages, 1)
		var data [][]byte
		if pError.UnConfirmedMessage != nil {
			data = pError.UnConfirmedMessage.Message.GetData()
		}

		err := fmt.Errorf("Failed to publish message: %s ,  error: %s. Total %d  \n", data, pError.Err, totalMessages)
		util.WriteError(r.log, errorCh, err)
	}
}

func (r *RabbitMQStreams) handleConfirm(confirms stream.ChannelPublishConfirm, wg *sync.WaitGroup) {
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

func (r *RabbitMQStreams) write(producer *stream.Producer, value []byte) error {
	if err := r.producer.Send(amqp.NewMessage(value)); err != nil {
		return errors.Wrapf(err, "unable to publish message to stream '%s'", r.Options.RabbitMQStreams.Stream)
	}

	return nil
}

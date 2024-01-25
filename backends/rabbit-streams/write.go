package rabbit_streams

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/validate"
)

func (r *RabbitStreams) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	// Make available to handleErr
	r.streamName = writeOpts.RabbitStreams.Args.Stream
	r.errorCh = errorCh

	producer, err := r.client.NewProducer(writeOpts.RabbitStreams.Args.Stream,
		stream.NewProducerOptions().
			SetProducerName(writeOpts.RedisStreams.Args.WriteId).
			SetBatchSize(len(messages)))

	if err != nil {
		return errors.Wrap(err, "unable to create rabbitmq streams producer")
	}

	defer producer.Close()

	chPublishConfirm := producer.NotifyPublishConfirmation()

	go r.handleConfirm(chPublishConfirm)

	for _, msg := range messages {
		if err := producer.Send(amqp.NewMessage([]byte(msg.Input))); err != nil {
			errorCh <- &records.ErrorRecord{
				Error: errors.Wrapf(err, "unable to publish message to stream '%s'",
					writeOpts.RabbitStreams.Args.Stream).Error(),
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			}
			continue
		}
		r.waitGroup.Add(1)
	}

	// Wait for confirmations
	r.waitGroup.Wait()

	return nil
}

func (r *RabbitStreams) handleConfirm(confirms stream.ChannelPublishConfirm) {
	for confirmed := range confirms {
		for _, msg := range confirmed {
			if msg.IsConfirmed() {
				continue
			} else {
				r.errorCh <- &records.ErrorRecord{
					Error:               fmt.Sprintf("Message failed: %s", msg.GetMessage()),
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				}
			}
			r.waitGroup.Done()
		}
	}
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.RabbitStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.RabbitStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.RabbitStreams.Args.Stream == "" {
		return errors.New("stream name cannot be empty")
	}

	if writeOpts.RabbitStreams.Args.Stream == "" {
		return ErrEmptyStream
	}

	return nil
}

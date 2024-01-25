package kubemq_queue

import (
	"context"
	"time"

	queuesStream "github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/validate"
)

func (k *KubeMQ) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	defer func() {
		_ = k.client.Close()
	}()

	var batch []*queuesStream.QueueMessage
	for _, msg := range messages {
		batch = append(batch, queuesStream.NewQueueMessage().
			SetChannel(writeOpts.KubemqQueue.Args.QueueName).
			SetBody([]byte(msg.Input)))
	}

	results, err := k.client.Send(context.Background(), batch...)
	if err != nil {
		return err
	}

	for i := 0; i < len(results.Results); i++ {
		if results.Results[i].IsError {
			errorCh <- &records.ErrorRecord{
				Error:               results.Results[i].Error,
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			}
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.KubemqQueue == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.KubemqQueue.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.KubemqQueue.Args.QueueName == "" {
		return errors.New("queue name cannot be empty")
	}

	return nil
}

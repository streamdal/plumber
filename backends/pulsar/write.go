package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (p *Pulsar) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: writeOpts.Pulsar.Args.Topic})
	if err != nil {
		return errors.Wrap(err, "unable to create Pulsar producer")
	}

	for _, msg := range messages {
		pm := &pulsar.ProducerMessage{
			Payload:   []byte(msg.Input),
			EventTime: time.Now().UTC(),
		}
		if _, err := producer.Send(ctx, pm); err != nil {
			errorCh <- &records.ErrorRecord{
				Error:               err.Error(),
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			}
		}
	}

	return nil
}

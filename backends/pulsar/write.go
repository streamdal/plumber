package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (p *Pulsar) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}
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
			util.WriteError(p.log, errorCh, errors.Wrapf(err, "unable to publish message to Pulsar"))
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.Pulsar == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.Pulsar.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}

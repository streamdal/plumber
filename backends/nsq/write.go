package nsq

import (
	"context"
	"fmt"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/util"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *NSQ) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	producer, err := nsq.NewProducer(n.connArgs.NsqdAddress, n.config)
	if err != nil {
		return errors.Wrap(err, "unable to start NSQ producer")
	}
	defer producer.Stop()

	producer.SetLogger(n.log, nsq.LogLevelError)

	topic := writeOpts.GetNsq().Args.Topic
	for _, value := range messages {
		if err := producer.Publish(topic, []byte(value.Input)); err != nil {
			util.WriteError(n.log.Entry, errorCh, fmt.Errorf("unable to write message to '%s': %s", topic, err))
			continue
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts.GetNsq().Args.Topic == "" {
		return ErrMissingTopic
	}

	return nil
}

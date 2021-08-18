package nsq

import (
	"context"

	"github.com/nsqio/go-nsq"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
)

// Write performs necessary setup and calls NSQ.Write() to write the actual message
func (n *NSQ) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := validateWriteOptions(n.Options); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	producer, err := n.createProducer()
	if err != nil {
		return errors.Wrap(err, "unable to instantiate NSQ producer")
	}

	for _, msg := range messages {
		if err := n.write(producer, msg.Value); err != nil {
			util.WriteError(n.log.Entry, errorCh, err)
		}
	}

	return nil
}

// Write publishes a message to a NSQ topic
func (n *NSQ) write(producer *nsq.Producer, value []byte) error {
	if err := producer.Publish(n.Options.NSQ.Topic, value); err != nil {
		return errors.Wrap(err, "unable to publish message to NSQ")
	}

	n.log.Infof("Successfully wrote message to '%s'", n.Options.NSQ.Topic)
	return nil
}

func validateWriteOptions(opts *options.Options) error {
	if opts.NSQ.TLSCAFile != "" || opts.NSQ.TLSClientCertFile != "" || opts.NSQ.TLSClientKeyFile != "" {
		if opts.NSQ.TLSClientKeyFile == "" {
			return ErrMissingTLSKey
		}

		if opts.NSQ.TLSClientCertFile == "" {
			return ErrMissingTlsCert
		}

		if opts.NSQ.TLSCAFile == "" {
			return ErrMissingTLSCA
		}
	}

	return nil
}

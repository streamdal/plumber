package nats_jetstream

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	cenats "github.com/cloudevents/sdk-go/protocol/nats/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *NatsJetstream) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	if writeOpts.EncodeOptions != nil && writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_CLOUDEVENT {
		return n.writeCloudEvents(ctx, writeOpts, errorCh, messages...)
	}

	jsCtx, err := n.client.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return errors.Wrap(err, "failed to get jetstream context")
	}

	stream := writeOpts.NatsJetstream.Args.Subject

	for _, msg := range messages {
		if _, err := jsCtx.Publish(stream, []byte(msg.Input)); err != nil {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to subject '%s': %s", stream, err))
			continue
		}
	}

	return nil
}

func (n *NatsJetstream) writeCloudEvents(_ context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	subject := writeOpts.NatsJetstream.Args.Subject

	sender, err := cenats.NewSenderFromConn(n.client, subject)
	if err != nil {
		return errors.Wrap(err, "unable to create new cloudevents sender")
	}

	// Not performing sender.Close() here since plumber handles connection closing

	c, err := cloudevents.NewClient(sender)
	if err != nil {
		return errors.Wrap(err, "failed to create cloudevents client")
	}

	for i, msg := range messages {
		e, err := util.GenCloudEvent(writeOpts.EncodeOptions.CloudeventSettings, msg)
		if err != nil {
			util.WriteError(n.log, errorCh, errors.Wrap(err, "unable to generate cloudevents event"))
			continue
		}

		result := c.Send(context.Background(), *e)
		if cloudevents.IsUndelivered(result) {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to subject '%s': %s", subject, result))
			continue
		}

		n.log.Debugf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.NatsJetstream == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.NatsJetstream.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}

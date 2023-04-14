package nats

import (
	"context"
	"fmt"
	"log"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	cenats "github.com/cloudevents/sdk-go/protocol/nats/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (n *Nats) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	if writeOpts.EncodeOptions != nil && writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_CLOUDEVENT {
		return n.writeCloudEvents(ctx, writeOpts, errorCh, messages...)
	}

	subject := writeOpts.Nats.Args.Subject

	for _, msg := range messages {
		if err := n.Client.Publish(subject, []byte(msg.Input)); err != nil {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to subject '%s': %s", subject, err))
			continue
		}
		return nil
	}

	return nil
}

func (n *Nats) writeCloudEvents(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	subject := writeOpts.Nats.Args.Subject

	sender, err := cenats.NewSenderFromConn(n.Client, subject)
	if err != nil {
		return errors.Wrap(err, "unable to create new cloudevents send")
	}

	// Not performing sender.Close() here since plumber handles connection closing

	c, err := cloudevents.NewClient(sender)
	if err != nil {
		log.Fatalf("Failed to create client, %s", err.Error())
	}

	for i, msg := range messages {
		e, err := util.GenCloudEvent(writeOpts.EncodeOptions.CloudeventSettings, msg)
		if err != nil {
			util.WriteError(n.log, errorCh, errors.Wrap(err, "unable to generate cloudevents event"))
			continue
		}

		if result := c.Send(ctx, *e); cloudevents.IsUndelivered(result) {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to subject '%s': %s", subject, result))
			continue
		} else {
			n.log.Debugf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.Nats == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.Nats.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.Nats.Args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}

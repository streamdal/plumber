package nats_streaming

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	cenats "github.com/cloudevents/sdk-go/protocol/nats/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (n *NatsStreaming) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	if writeOpts.EncodeOptions != nil && writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_CLOUDEVENT {
		return n.writeCloudEvents(ctx, writeOpts, errorCh, messages...)
	}

	for _, msg := range messages {
		err := n.stanClient.Publish(writeOpts.NatsStreaming.Args.Channel, []byte(msg.Input))
		if err != nil {
			util.WriteError(n.log, errorCh, errors.Wrap(err, "unable to publish nats-streaming message"))
			break
		}
	}

	return nil
}

func (n *NatsStreaming) writeCloudEvents(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	channel := writeOpts.NatsStreaming.Args.Channel

	sender, err := cenats.NewSenderFromConn(n.client, channel)
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

		result := c.Send(ctx, *e)

		if cloudevents.IsUndelivered(result) {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to channel '%s': %s", channel, result))
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

	if writeOpts.NatsStreaming == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.NatsStreaming.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.NatsStreaming.Args.Channel == "" {
		return ErrEmptyChannel
	}

	return nil
}

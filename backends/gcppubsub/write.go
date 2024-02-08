package gcppubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/validate"
)

func (g *GCPPubSub) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.New("unable to validate write options")
	}

	t := g.client.Topic(writeOpts.GcpPubsub.Args.TopicId)

	for _, msg := range messages {
		result := t.Publish(ctx, &pubsub.Message{
			Data: []byte(msg.Input),
		})

		if _, err := result.Get(ctx); err != nil {
			errorCh <- &records.ErrorRecord{
				Error:               err.Error(),
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			}
			continue
		}

		g.log.Infof("Successfully wrote message to topic '%s'", writeOpts.GcpPubsub.Args.TopicId)

	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.GcpPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.GcpPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.GcpPubsub.Args.TopicId == "" {
		return errors.New("Topic ID cannot be empty")
	}

	return nil
}

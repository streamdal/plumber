package activemq

import (
	"context"

	"github.com/pkg/errors"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *ActiveMQ) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	destination := getDestinationWrite(writeOpts.Activemq.Args)
	for _, msg := range messages {
		if err := a.client.Send(destination, "", []byte(msg.Input), nil); err != nil {
			util.WriteError(nil, errorCh, errors.Wrap(err, "unable to send ActiveMQ message"))
		}
	}

	return nil
}

func getDestinationWrite(args *args.ActiveMQWriteArgs) string {
	if args.Topic != "" {
		return "/topic/" + args.Topic
	}
	return args.Queue
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.Activemq == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.Activemq.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrTopicOrQueue
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrTopicAndQueue
	}

	return nil
}

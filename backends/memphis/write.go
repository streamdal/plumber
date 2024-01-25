package memphis

import (
	"context"
	"fmt"

	"github.com/memphisdev/memphis.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (m *Memphis) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to verify write options")
	}

	args := writeOpts.GetMemphis().Args

	producer, err := m.client.CreateProducer(args.Station, args.ProducerName)
	if err != nil {
		return errors.Wrap(err, "unable to create Memphis producer")
	}

	defer m.client.Close()

	headers := genHeaders(args.Headers)

	po := make([]memphis.ProduceOpt, 0)
	po = append(po, memphis.MsgHeaders(headers))

	if args.MessageId != "" {
		po = append(po, memphis.MsgId(args.MessageId))
	}

	for _, msg := range messages {
		if err := producer.Produce([]byte(msg.Input), po...); err != nil {
			util.WriteError(m.log, errorCh, fmt.Errorf("unable to write message to station '%s': %s", args.Station, err))
		}
	}

	return nil
}

func validateWriteOptions(opts *opts.WriteOptions) error {
	if opts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if opts.Memphis == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := opts.Memphis.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Station == "" {
		return ErrEmptyStation
	}

	return nil
}

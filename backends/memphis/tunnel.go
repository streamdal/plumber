package memphis

import (
	"context"
	"fmt"

	"github.com/memphisdev/memphis.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/tunnel"
	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

func (m *Memphis) Tunnel(ctx context.Context, opts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(opts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := m.log.WithField("pkg", "memphis/tunnel")

	args := opts.GetMemphis().Args

	producer, err := m.client.CreateProducer(args.Station, args.ProducerName)
	if err != nil {
		return errors.Wrap(err, "unable to create Memphis producer")
	}

	if err := tunnelSvc.Start(ctx, "Memphis", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	headers := genHeaders(args.Headers)

	po := make([]memphis.ProduceOpt, 0)
	po = append(po, memphis.MsgHeaders(headers))

	if args.MessageId != "" {
		po = append(po, memphis.MsgId(args.MessageId))
	}

	outboundCh := tunnelSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			if err := producer.Produce(outbound.Blob, po...); err != nil {
				util.WriteError(m.log, errorCh, fmt.Errorf("unable to write message to station '%s': %s", args.Station, err))
			}
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil

}

func validateTunnelOptions(opts *opts.TunnelOptions) error {
	if opts == nil {
		return validate.ErrEmptyTunnelOpts
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

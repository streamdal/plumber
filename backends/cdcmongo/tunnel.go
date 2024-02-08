package cdcmongo

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/tunnel"
	"github.com/streamdal/plumber/types"
)

func (m *Mongo) Tunnel(ctx context.Context, opts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	return types.NotImplementedErr
}

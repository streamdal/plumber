package kubemq_queue

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/types"
)

func (k *KubeMQ) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	return types.NotImplementedErr
}

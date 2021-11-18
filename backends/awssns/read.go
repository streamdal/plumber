package awssns

import (
	"context"

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *AWSSNS) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	return types.NotImplementedErr
}

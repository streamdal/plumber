package cdcmongo

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/types"
)

func (m *Mongo) Dynamic(ctx context.Context, opts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	return types.NotImplementedErr
}

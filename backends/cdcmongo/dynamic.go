package cdcmongo

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/types"
)

func (m *Mongo) Dynamic(ctx context.Context, opts *opts.DynamicOptions) error {
	return types.NotImplementedErr
}

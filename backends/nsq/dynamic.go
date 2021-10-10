package nsq

import (
	"context"

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (n *NSQ) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	return types.NotImplementedErr
}

package rabbit_streams

import (
	"context"

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (r *RabbitStreams) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	return types.NotImplementedErr
}

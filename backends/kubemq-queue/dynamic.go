package kubemq_queue

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
)

func (k *KubeMQ) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	return types.NotImplementedErr
}

package mqtt

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
)

func (m *MQTT) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	return types.NotImplementedErr
}

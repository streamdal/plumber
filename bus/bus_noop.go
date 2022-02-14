package bus

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

// NoOpBus is a bus that does nothing
type NoOpBus struct{}

func (n NoOpBus) Start(_ context.Context) error {
	return nil
}

func (n NoOpBus) Stop() error {
	return nil
}

func (n NoOpBus) PublishCreateConnection(_ context.Context, _ *opts.ConnectionOptions) error {
	return nil
}

func (n NoOpBus) PublishUpdateConnection(_ context.Context, _ *opts.ConnectionOptions) error {
	return nil
}

func (n NoOpBus) PublishDeleteConnection(_ context.Context, _ *opts.ConnectionOptions) error {
	return nil
}

func (n NoOpBus) PublishCreateRelay(_ context.Context, _ *opts.RelayOptions) error {
	return nil
}

func (n NoOpBus) PublishUpdateRelay(_ context.Context, _ *opts.RelayOptions) error {
	return nil
}

func (n NoOpBus) PublishDeleteRelay(_ context.Context, _ *opts.RelayOptions) error {
	return nil
}

func (n NoOpBus) PublishStopRelay(_ context.Context, _ *opts.RelayOptions) error {
	return nil
}

func (n NoOpBus) PublishResumeRelay(_ context.Context, _ *opts.RelayOptions) error {
	return nil
}

func (n NoOpBus) PublishCreateDynamic(_ context.Context, _ *opts.DynamicOptions) error {
	return nil
}

func (n NoOpBus) PublishUpdateDynamic(_ context.Context, _ *opts.DynamicOptions) error {
	return nil
}

func (n NoOpBus) PublishStopDynamic(_ context.Context, _ *opts.DynamicOptions) error {
	return nil
}

func (n NoOpBus) PublishResumeDynamic(_ context.Context, _ *opts.DynamicOptions) error {
	return nil
}

func (n NoOpBus) PublishDeleteDynamic(_ context.Context, _ *opts.DynamicOptions) error {
	return nil
}

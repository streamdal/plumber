package bus

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

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

func (n NoOpBus) PublishCreateTunnel(_ context.Context, _ *opts.TunnelOptions) error {
	return nil
}

func (n NoOpBus) PublishUpdateTunnel(_ context.Context, _ *opts.TunnelOptions) error {
	return nil
}

func (n NoOpBus) PublishStopTunnel(_ context.Context, _ *opts.TunnelOptions) error {
	return nil
}

func (n NoOpBus) PublishResumeTunnel(_ context.Context, _ *opts.TunnelOptions) error {
	return nil
}

func (n NoOpBus) PublishDeleteTunnel(_ context.Context, _ *opts.TunnelOptions) error {
	return nil
}

func (n NoOpBus) PublishCreateRuleSet(_ context.Context, _ *common.RuleSet) error {
	return nil
}

func (n NoOpBus) PublishUpdateRuleSet(_ context.Context, _ *common.RuleSet) error {
	return nil
}

func (n NoOpBus) PublishDeleteRuleSet(_ context.Context, _ *common.RuleSet) error {
	return nil
}

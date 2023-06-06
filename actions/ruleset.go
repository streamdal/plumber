package actions

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
)

func (a *Actions) CreateRuleSet(ctx context.Context, rs *common.RuleSet) (*types.RuleSet, error) {
	if err := validate.RuleSetForServer(rs); err != nil {
		return nil, errors.Wrap(err, "unable to validate rule set")
	}

	set := &types.RuleSet{Set: rs}

	a.cfg.PersistentConfig.SetRuleSet(rs.Id, set)
	a.cfg.PersistentConfig.Save()

	return set, nil
}

func (a *Actions) UpdateRuleSet(ctx context.Context, id string, rs *common.RuleSet) (*types.RuleSet, error) {
	if err := validate.RuleSetForServer(rs); err != nil {
		return nil, errors.Wrap(err, "unable to validate rule set")
	}

	set := &types.RuleSet{Set: rs}

	a.cfg.PersistentConfig.SetRuleSet(rs.Id, set)
	a.cfg.PersistentConfig.Save()

	return set, nil
}

func (a *Actions) DeleteRuleSet(ctx context.Context, id string) (*types.RuleSet, error) {
	set := a.cfg.PersistentConfig.GetRuleSet(id)

	if set == nil {
		return nil, errors.New("unable to find rule set")
	}

	a.cfg.PersistentConfig.DeleteRuleSet(id)
	a.cfg.PersistentConfig.Save()

	return set, nil
}

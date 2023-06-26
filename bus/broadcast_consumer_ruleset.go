package bus

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/validate"
)

func (b *Bus) doCreateRuleSet(ctx context.Context, msg *Message) error {
	b.log.Debug("Received CreateRuleSet broadcast message")

	rs := &common.RuleSet{}
	if err := proto.Unmarshal(msg.Data, rs); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.RuleSet")
	}

	if err := validate.RuleSetForServer(rs); err != nil {
		return errors.Wrap(err, "rule set option validation failed")
	}

	if _, err := b.config.Actions.CreateRuleSet(ctx, rs); err != nil {
		return errors.Wrap(err, "unable to create rule set")
	}

	b.log.Infof("Created rule set '%s' (from broadcast msg)", rs.Id)

	return nil
}

func (b *Bus) doUpdateRuleSet(ctx context.Context, msg *Message) error {
	b.log.Debug("Received UpdateRuleSet broadcast message")

	rs := &common.RuleSet{}
	if err := proto.Unmarshal(msg.Data, rs); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.RuleSet")
	}

	if err := validate.RuleSetForServer(rs); err != nil {
		return errors.Wrap(err, "rule set option validation failed")
	}

	if _, err := b.config.Actions.UpdateRuleSet(ctx, rs.Id, rs); err != nil {
		return fmt.Errorf("unable to update rule set '%s': %s", rs.Id, err)
	}

	b.log.Infof("Updated rule set '%s' (from broadcast msg)", rs.Id)

	return nil
}

func (b *Bus) doDeleteRuleSet(ctx context.Context, msg *Message) error {
	b.log.Debug("Received DeleteRuleSet broadcast message")
	
	rs := &common.RuleSet{}
	if err := proto.Unmarshal(msg.Data, rs); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.RuleSet")
	}

	if rs.Id == "" {
		return errors.New("rule set id in options cannot be empty")
	}

	if _, err := b.config.Actions.DeleteRuleSet(ctx, rs.Id); err != nil {
		return fmt.Errorf("unable to delete rule set '%s': %s", rs.Id, err)
	}

	b.log.Infof("Deleted rule set '%s' (from broadcast msg)", rs.Id)

	return nil
}

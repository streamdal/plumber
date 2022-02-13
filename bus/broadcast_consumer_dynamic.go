package bus

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

func (b *Bus) doCreateDynamic(ctx context.Context, msg *Message) error {
	dynamicOptions := &opts.DynamicOptions{}
	if err := proto.Unmarshal(msg.Data, dynamicOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.DynamicOptions")
	}

	if err := validate.DynamicOptionsForServer(dynamicOptions); err != nil {
		return errors.Wrap(err, "dynamic option validation failed")
	}

	if _, err := b.config.Actions.CreateDynamic(ctx, dynamicOptions); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	b.log.Infof("Created dynamic '%s' (from broadcast msg)", dynamicOptions.XDynamicId)

	return nil
}

func (b *Bus) doUpdateDynamic(ctx context.Context, msg *Message) error {
	dynamicOptions := &opts.DynamicOptions{}
	if err := proto.Unmarshal(msg.Data, dynamicOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.DynamicOptions")
	}

	if dynamicOptions.XDynamicId == "" {
		return errors.New("dynamic id in options cannot be empty")
	}

	if _, err := b.config.Actions.UpdateDynamic(ctx, dynamicOptions.XDynamicId, dynamicOptions); err != nil {
		return fmt.Errorf("unable to update dynamic '%s': %s", dynamicOptions.XDynamicId, err)
	}

	b.log.Infof("Updated dynamic '%s' (from broadcast msg)", dynamicOptions.XDynamicId)

	return nil
}

func (b *Bus) doStopDynamic(ctx context.Context, msg *Message) error {
	// Only unmarshalling to get XDynamicID - we'll be operating off of what's in
	// our cache.
	dynamicOptions := &opts.DynamicOptions{}
	if err := proto.Unmarshal(msg.Data, dynamicOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.DynamicOptions")
	}

	if dynamicOptions.XDynamicId == "" {
		return errors.New("dynamic id in options cannot be empty")
	}

	if _, err := b.config.Actions.StopDynamic(ctx, dynamicOptions.XDynamicId); err != nil {
		return fmt.Errorf("unable to stop dynamic '%s': %s", dynamicOptions.XDynamicId, err)
	}

	b.log.Infof("Stopped dynamic '%s' (from broadcast msg)", dynamicOptions.XDynamicId)

	return nil
}

func (b *Bus) doResumeDynamic(ctx context.Context, msg *Message) error {
	// Only unmarshalling to get DynamicId - we'll be operating off of what's in
	// our cache.
	dynamicOptions := &opts.DynamicOptions{}
	if err := proto.Unmarshal(msg.Data, dynamicOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.DynamicOptions")
	}

	if dynamicOptions.XDynamicId == "" {
		return errors.New("dynamic id in options cannot be empty")
	}

	if _, err := b.config.Actions.ResumeDynamic(ctx, dynamicOptions.XDynamicId); err != nil {
		return fmt.Errorf("unable to resume dynamic '%s': %s", dynamicOptions.XDynamicId, err)
	}

	b.log.Infof("Resumed dynamic '%s' (from broadcast msg)", dynamicOptions.XDynamicId)

	return nil
}

func (b *Bus) doDeleteDynamic(ctx context.Context, msg *Message) error {
	// Only unmarshalling to get XDynamicId - we'll be operating off of what's in
	// our cache.
	dynamicOptions := &opts.DynamicOptions{}
	if err := proto.Unmarshal(msg.Data, dynamicOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.DynamicOptions")
	}

	if dynamicOptions.XDynamicId == "" {
		return errors.New("dynamic id in options cannot be empty")
	}

	if err := b.config.Actions.DeleteDynamic(ctx, dynamicOptions.XDynamicId); err != nil {
		return fmt.Errorf("unable to delete dynamic '%s': %s", dynamicOptions.XDynamicId, err)
	}

	b.log.Infof("Deleted dynamic '%s' (from broadcast msg)", dynamicOptions.XDynamicId)

	return nil
}

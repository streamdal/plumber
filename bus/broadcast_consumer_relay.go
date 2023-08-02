package bus

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/validate"
)

func (b *Bus) doCreateRelay(ctx context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.RelayOptions")
	}

	if err := validate.RelayOptionsForServer(relayOptions); err != nil {
		return errors.Wrap(err, "relay option validation failed")
	}

	if _, err := b.config.Actions.CreateRelay(ctx, relayOptions); err != nil {
		return errors.Wrap(err, "unable to create relay")
	}

	b.log.Infof("created relay '%s' (from broadcast msg)", relayOptions.XRelayId)

	return nil
}

func (b *Bus) doStopRelay(ctx context.Context, msg *Message) error {
	// Only unmarshalling to get XRelayId - we'll be operating off of what's in
	// our cache.
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.RelayOptions")
	}

	if relayOptions.XRelayId == "" {
		return errors.New("relay id in options cannot be empty")
	}

	if _, err := b.config.Actions.StopRelay(ctx, relayOptions.XRelayId); err != nil {
		return fmt.Errorf("unable to stop relay '%s': %s", relayOptions.XRelayId, err)
	}

	b.log.Infof("stopped relay '%s' (from broadcast msg)", relayOptions.XRelayId)

	return nil
}

func (b *Bus) doResumeRelay(ctx context.Context, msg *Message) error {
	// Only unmarshalling to get XRelayId - we'll be operating off of what's in
	// our cache.
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.RelayOptions")
	}

	if relayOptions.XRelayId == "" {
		return errors.New("relay id in options cannot be empty")
	}

	if _, err := b.config.Actions.ResumeRelay(ctx, relayOptions.XRelayId); err != nil {
		return fmt.Errorf("unable to resume relay '%s': %s", relayOptions.XRelayId, err)
	}

	b.log.Infof("resumed relay '%s' (from broadcast msg)", relayOptions.XRelayId)

	return nil
}

func (b *Bus) doUpdateRelay(ctx context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.RelayOptions")
	}

	if relayOptions.XRelayId == "" {
		return errors.New("relay id in options cannot be empty")
	}

	if _, err := b.config.Actions.UpdateRelay(ctx, relayOptions.XRelayId, relayOptions); err != nil {
		return fmt.Errorf("unable to create relay '%s': %s", relayOptions.XRelayId, err)
	}

	return nil
}

func (b *Bus) doDeleteRelay(ctx context.Context, msg *Message) error {
	// Only unmarshalling to get XRelayId - we'll be operating off of what's in
	// our cache.
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.RelayOptions")
	}

	if relayOptions.XRelayId == "" {
		return errors.New("relay id in options cannot be empty")
	}

	if _, err := b.config.Actions.DeleteRelay(ctx, relayOptions.XRelayId); err != nil {
		return fmt.Errorf("unable to delete relay '%s': %s", relayOptions.XRelayId, err)
	}

	b.log.Infof("deleted relay '%s' (from broadcast msg)", relayOptions.XRelayId)

	return nil
}

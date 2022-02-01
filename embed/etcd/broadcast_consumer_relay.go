package etcd

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

func (e *Etcd) doCreateRelay(ctx context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Relay")
	}

	if err := validate.RelayOptionsForServer(relayOptions); err != nil {
		return errors.Wrap(err, "relay option vailidation failed")
	}

	r, err := e.Actions.CreateRelay(ctx, relayOptions)
	if err != nil {
		return errors.Wrap(err, "unable to create relay")
	}

	// Set in config map
	e.PersistentConfig.SetRelay(relayOptions.XRelayId, r)

	e.log.Debugf("created relay '%s' (from broadcast msg)", relayOptions.XRelayId)

	return nil
}

// TODO: Implement
func (e *Etcd) doStopRelay(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (e *Etcd) doResumeRelay(_ context.Context, msg *Message) error {
	panic("implement me")
}

func (e *Etcd) doUpdateRelay(_ context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Relay")
	}

	// Set in config map
	e.PersistentConfig.SetRelay(relayOptions.XRelayId, &types.Relay{Options: relayOptions})

	e.log.Debugf("updated relay '%s'", relayOptions.XRelayId)

	// TODO: Should restart relay (if active)

	return nil
}

func (e *Etcd) doDeleteRelay(_ context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Relay")
	}

	// Set in config map
	e.PersistentConfig.DeleteRelay(relayOptions.XRelayId)

	e.log.Debugf("deleted relay '%s'", relayOptions.XRelayId)

	// TODO: Delete should stop the running relay (if active)

	return nil
}

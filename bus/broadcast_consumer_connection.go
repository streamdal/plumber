package bus

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/server/types"
)

func (b *Bus) doCreateConnection(_ context.Context, msg *Message) error {
	b.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Save connection to in-memory map
	b.config.PersistentConfig.SetConnection(connOpts.XId, &types.Connection{
		Connection: connOpts,
	})
	b.config.PersistentConfig.Save()

	b.log.Infof("created connection '%s' (from broadcast)", connOpts.Name)

	return nil
}

func (b *Bus) doUpdateConnection(_ context.Context, msg *Message) error {
	b.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Update connection in in-memory map
	b.config.PersistentConfig.SetConnection(connOpts.XId, &types.Connection{
		Connection: connOpts,
	})

	b.log.Debugf("updated connection '%s'", connOpts.Name)

	// TODO: some way to signal reads/relays to restart? How will GRPC streams handle this?

	// TODO: Some more work here

	return nil
}

func (b *Bus) doDeleteConnection(ctx context.Context, msg *Message) error {
	b.config.PersistentConfig.DynamicReplaysMutex.RLock()
	defer b.config.PersistentConfig.DynamicReplaysMutex.RUnlock()
	b.config.PersistentConfig.RelaysMutex.RLock()
	defer b.config.PersistentConfig.RelaysMutex.RUnlock()

	// Ensure this connection isn't being used by any dynamic replays
	for id, dynamicReplay := range b.config.PersistentConfig.Dynamic {
		if dynamicReplay.Options.ConnectionId == id {
			return fmt.Errorf("cannot delete connection '%s' because it is in use by dynamic replay '%s'",
				id, dynamicReplay.Options.XDynamicId)
		}
	}

	// Ensure this connection isn't being used by any relays

	for id, dynamicReplay := range b.config.PersistentConfig.Relays {
		if dynamicReplay.Options.ConnectionId == id {
			return fmt.Errorf("cannot delete connection '%s' because it is in use by relay '%s'",
				id, dynamicReplay.Options.XRelayId)
		}
	}

	b.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	if connOpts.XId == "" {
		return errors.New("connection XId cannot be empty")
	}

	existingConn := b.config.PersistentConfig.GetConnection(connOpts.XId)
	if existingConn == nil {
		return fmt.Errorf("connection id '%s' does not exist", connOpts.XId)
	}

	// Stop any relays that use this connection
	//for relayID, relayCfg := range b.config.PersistentConfig.Relays {
	//	if relayCfg.Options.ConnectionId == connOpts.XId {
	//		b.log.Infof("attempting to delete relay '%s' that uses connection '%s'", relayID, connOpts.XId)
	//
	//		if _, err := b.config.Actions.DeleteRelay(ctx, relayID); err != nil {
	//			return errors.Wrapf(err, "unable to delete relay '%s'; troubleshoot and perform manual deletes", relayID)
	//		}
	//	}
	//}
	//
	//// Stop any dynamic that use this connection
	//for dynamicID, dynamicCfg := range b.config.PersistentConfig.Dynamic {
	//	if dynamicCfg.Options.ConnectionId == connOpts.XId {
	//		b.log.Infof("attempting to delete dynamic '%s' that uses connection '%s'", dynamicID, connOpts.XId)
	//
	//		if err := b.config.Actions.DeleteDynamic(ctx, dynamicID); err != nil {
	//			return errors.Wrapf(err, "unable to delete dynamic '%s'; troubleshoot and perform manual deletes", dynamicID)
	//		}
	//	}
	//}

	// Delete connection from config
	b.config.PersistentConfig.DeleteConnection(connOpts.XId)
	b.config.PersistentConfig.Save()

	b.log.Debugf("deleted connection '%s'", connOpts.Name)

	return nil
}

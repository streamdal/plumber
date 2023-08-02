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
		return errors.Wrap(err, "unable to unmarshal message into opts.ConnectionOptions")
	}

	// Save connection to in-memory map
	b.config.PersistentConfig.SetConnection(connOpts.XId, &types.Connection{
		Connection: connOpts,
	})
	b.config.PersistentConfig.Save()

	b.log.Infof("created connection '%s' (from broadcast)", connOpts.Name)

	return nil
}

func (b *Bus) doUpdateConnection(ctx context.Context, msg *Message) error {
	b.log.Debugf("running doUpdateonnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ConnectionOptions")
	}

	if _, err := b.config.Actions.UpdateConnection(ctx, connOpts.XId, connOpts); err != nil {
		return errors.Wrap(err, "unable to update connection")
	}

	b.log.Debugf("updated connection '%s'", connOpts.Name)

	return nil
}

func (b *Bus) doDeleteConnection(ctx context.Context, msg *Message) error {
	b.config.PersistentConfig.TunnelsMutex.RLock()
	defer b.config.PersistentConfig.TunnelsMutex.RUnlock()
	b.config.PersistentConfig.RelaysMutex.RLock()
	defer b.config.PersistentConfig.RelaysMutex.RUnlock()

	// Ensure this connection isn't being used by any tunnels
	for id, tunnel := range b.config.PersistentConfig.Tunnels {
		if tunnel.Options.ConnectionId == id {
			return fmt.Errorf("cannot delete connection '%s' because it is in use by tunnel '%s'",
				id, tunnel.Options.XTunnelId)
		}
	}

	// Ensure this connection isn't being used by any relays
	for id, relay := range b.config.PersistentConfig.Relays {
		if relay.Options.ConnectionId == id {
			return fmt.Errorf("cannot delete connection '%s' because it is in use by relay '%s'",
				id, relay.Options.XRelayId)
		}
	}

	b.log.Debugf("running doDeleteConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ConnectionOptions")
	}

	if connOpts.XId == "" {
		return errors.New("connection XId cannot be empty")
	}

	existingConn := b.config.PersistentConfig.GetConnection(connOpts.XId)
	if existingConn == nil {
		return fmt.Errorf("connection id '%s' does not exist", connOpts.XId)
	}

	// TODO: Verify that the connection is not used by anything - if it is, return an error

	// Delete connection from config
	b.config.PersistentConfig.DeleteConnection(connOpts.XId)
	b.config.PersistentConfig.Save()

	b.log.Debugf("deleted connection '%s'", connOpts.Name)

	return nil
}

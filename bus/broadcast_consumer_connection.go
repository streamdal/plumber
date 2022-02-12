package bus

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/server/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

func (b *Bus) doCreateConnection(_ context.Context, msg *Message) error {
	b.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	// TODO: Validate messagea

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Save connection to in-memory map
	b.config.PersistentConfig.SetConnection(connOpts.XId, &types.Connection{
		Connection: connOpts,
	})

	b.log.Debugf("created connection '%s'", connOpts.Name)

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

	return nil
}

func (b *Bus) doDeleteConnection(_ context.Context, msg *Message) error {
	b.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Delete connOptsection
	b.config.PersistentConfig.DeleteConnection(connOpts.XId)

	b.log.Debugf("deleted connection '%s'", connOpts.Name)

	// TODO: stop reads/relays from this connection

	return nil
}

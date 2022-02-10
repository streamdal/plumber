package bus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/server/types"
)

// IMPORTANT: Returning an error will cause the consumer to stop
func (b *Bus) broadcastCallback(ctx context.Context, natsMsg *nats.Msg) error {
	if natsMsg == nil {
		return errors.New("response cannot be nil")
	}

	msg := &Message{}

	if err := json.Unmarshal(natsMsg.Data, msg); err != nil {
		b.log.Errorf("unable to unmarshal NATS message on subj '%s': %s", natsMsg.Subject, err)
		return nil
	}

	if msg.EmittedBy == b.config.ServerOptions.NodeId {
		b.log.Debugf("ignoring message emitted by self")
		return nil
	}

	var err error

	// Add actions here that the consumer should respond to
	switch msg.Action {
	// Connection
	case CreateConnection:
		err = b.doCreateConnection(ctx, msg)
	case UpdateConnection:
		err = b.doUpdateConnection(ctx, msg)
	case DeleteConnection:
		err = b.doDeleteConnection(ctx, msg)

	// Service
	case CreateService:
		err = b.doCreateService(ctx, msg)
	case UpdateService:
		err = b.doUpdateService(ctx, msg)
	case DeleteService:
		err = b.doDeleteService(ctx, msg)

	// Schema
	case CreateSchema:
		err = b.doCreateSchema(ctx, msg)
	case UpdateSchema:
		err = b.doUpdateSchema(ctx, msg)
	case DeleteSchema:
		err = b.doDeleteSchema(ctx, msg)

	// Relay
	case CreateRelay:
		err = b.doCreateRelay(ctx, msg)
	case UpdateRelay:
		err = b.doUpdateRelay(ctx, msg)
	case DeleteRelay:
		err = b.doDeleteRelay(ctx, msg)
	case StopRelay:
		err = b.doStopRelay(ctx, msg)
	case ResumeRelay:
		err = b.doResumeRelay(ctx, msg)

	// Dynamic
	case CreateDynamic:
		err = b.doCreateDynamic(ctx, msg)
	case UpdateDynamic:
		err = b.doUpdateDynamic(ctx, msg)
	case DeleteDynamic:
		err = b.doDeleteDynamic(ctx, msg)
	case StopDynamic:
		err = b.doStopDynamic(ctx, msg)
	case ResumeDynamic:
		err = b.doResumeDynamic(ctx, msg)

	// Config
	case UpdateConfig:
		err = b.doUpdateConfig(ctx, msg)

	// Validation
	case CreateValidation:
		err = b.doCreateValidation(ctx, msg)
	case UpdateValidation:
		err = b.doUpdateValidation(ctx, msg)
	case DeleteValidation:
		err = b.doDeleteValidation(ctx, msg)

	// Read
	case CreateRead:
		err = b.doCreateRead(ctx, msg)
	case DeleteRead:
		err = b.doDeleteRead(ctx, msg)
	default:
		b.log.Debugf("unrecognized action '%s' in msg on subj '%s' - skipping", msg.Action, natsMsg.Subject)
	}

	if err != nil {

	}

	return nil
}

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

func (b *Bus) doCreateService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	b.config.PersistentConfig.SetService(svc.Id, svc)

	b.log.Debugf("updated service '%s'", svc.Name)

	return nil
}

func (b *Bus) doUpdateService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	b.config.PersistentConfig.SetService(svc.Id, svc)

	b.log.Debugf("updated service '%s'", svc.Name)

	return nil
}

func (b *Bus) doDeleteService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	b.config.PersistentConfig.DeleteService(svc.Id)

	b.log.Debugf("deleted service '%s'", svc.Name)

	return nil
}

func (b *Bus) doCreateSchema(_ context.Context, msg *Message) error {
	schema := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, schema); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	b.config.PersistentConfig.SetSchema(schema.Id, schema)

	b.log.Debugf("updated schema '%s'", schema.Name)

	return nil
}

func (b *Bus) doUpdateSchema(_ context.Context, msg *Message) error {
	schema := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, schema); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	b.config.PersistentConfig.SetSchema(schema.Id, schema)

	b.log.Debugf("updated schema '%s'", schema.Name)

	return nil
}

func (b *Bus) doDeleteSchema(_ context.Context, msg *Message) error {
	svc := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	b.config.PersistentConfig.DeleteSchema(svc.Id)

	b.log.Debugf("deleted schema '%s'", svc.Name)

	return nil
}

// TODO: Implement
func (b *Bus) doCreateDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (b *Bus) doDeleteDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (b *Bus) doUpdateDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (b *Bus) doStopDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (b *Bus) doResumeDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

func (b *Bus) doCreateValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	b.config.PersistentConfig.SetValidation(validation.XId, validation)

	b.log.Debugf("created validation '%s'", validation.XId)

	return nil
}

func (b *Bus) doUpdateValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	b.config.PersistentConfig.SetValidation(validation.XId, validation)

	b.log.Debugf("updated validation '%s'", validation.XId)

	return nil
}

func (b *Bus) doDeleteValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	b.config.PersistentConfig.DeleteValidation(validation.XId)

	b.log.Debugf("deleted validation '%s'", validation.XId)

	return nil
}

func (b *Bus) doCreateRead(_ context.Context, msg *Message) error {
	readOpts := &opts.ReadOptions{}
	if err := proto.Unmarshal(msg.Data, readOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ReadOptions")
	}

	if err := b.populateDecodeSchemaDetails(readOpts); err != nil {
		return fmt.Errorf("unable to create readOpts '%s' from cache: %s", readOpts.XId, err)
	}

	readOpts.XActive = false

	read, err := types.NewRead(&types.ReadConfig{
		ReadOptions: readOpts,
		PlumberID:   b.config.PersistentConfig.PlumberID,
		Backend:     nil, // intentionally nil
	})
	if err != nil {
		return err
	}

	// Set in config map
	b.config.PersistentConfig.SetRead(readOpts.XId, read)

	b.log.Debugf("created readOpts '%s'", readOpts.XId)

	return nil
}

func (b *Bus) doDeleteRead(_ context.Context, msg *Message) error {
	read := &opts.ReadOptions{}
	if err := proto.Unmarshal(msg.Data, read); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ReadOptions")
	}

	// Set in config map
	b.config.PersistentConfig.DeleteRead(read.XId)

	b.log.Debugf("deleted read '%s'", read.XId)

	return nil
}

func (b *Bus) doUpdateConfig(_ context.Context, msg *Message) error {
	updateCfg := &MessageUpdateConfig{}
	if err := json.Unmarshal(msg.Data, updateCfg); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into MessageUpdateConfig")
	}

	// Set in config map
	b.config.PersistentConfig.VCServiceToken = updateCfg.VCServiceToken
	b.config.PersistentConfig.GitHubToken = updateCfg.GithubToken

	b.log.Debugf("updated config via MessageUpdateConfig")

	return nil
}

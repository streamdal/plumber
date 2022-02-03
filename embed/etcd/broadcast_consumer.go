package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/server/types"
)

func (e *Etcd) handleBroadcastWatchResponse(ctx context.Context, resp *clientv3.WatchResponse) error {
	if resp == nil {
		return errors.New("response cannot be nil")
	}

	for _, v := range resp.Events {
		e.log.Debugf("got a new '%s' etcd message: %s", v.Type, string(v.Kv.Value))

		// We only care about creations
		if v.Type != clientv3.EventTypePut {
			continue
		}

		msg := &Message{}

		if err := json.Unmarshal(v.Kv.Value, msg); err != nil {
			e.log.Errorf("unable to unmarshal etcd key '%s' to message: %s", string(v.Kv.Key), err)
			continue
		}

		if msg.EmittedBy == e.PersistentConfig.PlumberID {
			e.log.Debugf("ignoring message emitted by self")
			continue
		}

		var err error

		// Add actions here that the consumer should respond to
		switch msg.Action {
		// Connection
		case CreateConnection:
			err = e.doCreateConnection(ctx, msg)
		case UpdateConnection:
			err = e.doUpdateConnection(ctx, msg)
		case DeleteConnection:
			err = e.doDeleteConnection(ctx, msg)

		// Service
		case CreateService:
			err = e.doCreateService(ctx, msg)
		case UpdateService:
			err = e.doUpdateService(ctx, msg)
		case DeleteService:
			err = e.doDeleteService(ctx, msg)

		// Schema
		case CreateSchema:
			err = e.doCreateSchema(ctx, msg)
		case UpdateSchema:
			err = e.doUpdateSchema(ctx, msg)
		case DeleteSchema:
			err = e.doDeleteSchema(ctx, msg)

		// Relay
		case CreateRelay:
			err = e.doCreateRelay(ctx, msg)
		case UpdateRelay:
			err = e.doUpdateRelay(ctx, msg)
		case DeleteRelay:
			err = e.doDeleteRelay(ctx, msg)
		case StopRelay:
			err = e.doStopRelay(ctx, msg)
		case ResumeRelay:
			err = e.doResumeRelay(ctx, msg)

		// Dynamic
		case CreateDynamic:
			err = e.doCreateDynamic(ctx, msg)
		case UpdateDynamic:
			err = e.doUpdateDynamic(ctx, msg)
		case DeleteDynamic:
			err = e.doDeleteDynamic(ctx, msg)
		case StopDynamic:
			err = e.doStopDynamic(ctx, msg)
		case ResumeDynamic:
			err = e.doResumeDynamic(ctx, msg)

		// Config
		case UpdateConfig:
			err = e.doUpdateConfig(ctx, msg)

		// Validation
		case CreateValidation:
			err = e.doCreateValidation(ctx, msg)
		case UpdateValidation:
			err = e.doUpdateValidation(ctx, msg)
		case DeleteValidation:
			err = e.doDeleteValidation(ctx, msg)

		// Read
		case CreateRead:
			err = e.doCreateRead(ctx, msg)
		case DeleteRead:
			err = e.doDeleteRead(ctx, msg)
		default:
			e.log.Debugf("unrecognized action '%s' for key '%s' - skipping", msg.Action, string(v.Kv.Key))
		}

		if err != nil {
			e.log.Errorf("unable to complete '%s' action for key '%s': %s",
				msg.Action, string(v.Kv.Key), err)
		}
	}

	return nil
}

func (e *Etcd) doCreateConnection(_ context.Context, msg *Message) error {
	e.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	// TODO: Validate the message

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Save connection to in-memory map
	e.PersistentConfig.SetConnection(connOpts.XId, connOpts)

	e.log.Debugf("created connection '%s'", connOpts.Name)

	return nil
}

func (e *Etcd) doUpdateConnection(_ context.Context, msg *Message) error {
	e.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Update connection in in-memory map
	e.PersistentConfig.SetConnection(connOpts.XId, connOpts)

	e.log.Debugf("updated connection '%s'", connOpts.Name)

	// TODO: some way to signal reads/relays to restart? How will GRPC streams handle this?

	return nil
}

func (e *Etcd) doDeleteConnection(_ context.Context, msg *Message) error {
	e.log.Debugf("running doCreateConnection handler for msg emitted by %s", msg.EmittedBy)

	connOpts := &opts.ConnectionOptions{}
	if err := proto.Unmarshal(msg.Data, connOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Connection")
	}

	// Delete connOptsection
	e.PersistentConfig.DeleteConnection(connOpts.XId)

	e.log.Debugf("deleted connection '%s'", connOpts.Name)

	// TODO: stop reads/relays from this connection

	return nil
}

func (e *Etcd) doCreateService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	e.PersistentConfig.SetService(svc.Id, svc)

	e.log.Debugf("updated service '%s'", svc.Name)

	return nil
}

func (e *Etcd) doUpdateService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	e.PersistentConfig.SetService(svc.Id, svc)

	e.log.Debugf("updated service '%s'", svc.Name)

	return nil
}

func (e *Etcd) doDeleteService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	e.PersistentConfig.DeleteService(svc.Id)

	e.log.Debugf("deleted service '%s'", svc.Name)

	return nil
}

func (e *Etcd) doCreateSchema(_ context.Context, msg *Message) error {
	schema := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, schema); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	e.PersistentConfig.SetSchema(schema.Id, schema)

	e.log.Debugf("updated schema '%s'", schema.Name)

	return nil
}

func (e *Etcd) doUpdateSchema(_ context.Context, msg *Message) error {
	schema := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, schema); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	e.PersistentConfig.SetSchema(schema.Id, schema)

	e.log.Debugf("updated schema '%s'", schema.Name)

	return nil
}

func (e *Etcd) doDeleteSchema(_ context.Context, msg *Message) error {
	svc := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	e.PersistentConfig.DeleteSchema(svc.Id)

	e.log.Debugf("deleted schema '%s'", svc.Name)

	return nil
}

// TODO: Implement
func (e *Etcd) doCreateDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (e *Etcd) doDeleteDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (e *Etcd) doUpdateDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (e *Etcd) doStopDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

// TODO: Implement
func (e *Etcd) doResumeDynamic(_ context.Context, msg *Message) error {
	panic("implement me")
}

func (e *Etcd) doCreateValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	e.PersistentConfig.SetValidation(validation.XId, validation)

	e.log.Debugf("created validation '%s'", validation.XId)

	return nil
}

func (e *Etcd) doUpdateValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	e.PersistentConfig.SetValidation(validation.XId, validation)

	e.log.Debugf("updated validation '%s'", validation.XId)

	return nil
}

func (e *Etcd) doDeleteValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	e.PersistentConfig.DeleteValidation(validation.XId)

	e.log.Debugf("deleted validation '%s'", validation.XId)

	return nil
}

func (e *Etcd) doCreateRead(_ context.Context, msg *Message) error {
	readOpts := &opts.ReadOptions{}
	if err := proto.Unmarshal(msg.Data, readOpts); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ReadOptions")
	}

	if err := e.populateDecodeSchemaDetails(readOpts); err != nil {
		return fmt.Errorf("unable to create readOpts '%s' from cache: %s", readOpts.XId, err)
	}

	readOpts.XActive = false

	read, err := types.NewRead(&types.ReadConfig{
		ReadOptions: readOpts,
		PlumberID:   e.PersistentConfig.PlumberID,
		Backend:     nil, // intentionally nil
	})
	if err != nil {
		return err
	}

	// Set in config map
	e.PersistentConfig.SetRead(readOpts.XId, read)

	e.log.Debugf("created readOpts '%s'", readOpts.XId)

	return nil
}

func (e *Etcd) doDeleteRead(_ context.Context, msg *Message) error {
	read := &opts.ReadOptions{}
	if err := proto.Unmarshal(msg.Data, read); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ReadOptions")
	}

	// Set in config map
	e.PersistentConfig.DeleteRead(read.XId)

	e.log.Debugf("deleted read '%s'", read.XId)

	return nil
}

func (e *Etcd) doUpdateConfig(_ context.Context, msg *Message) error {
	updateCfg := &MessageUpdateConfig{}
	if err := json.Unmarshal(msg.Data, updateCfg); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into MessageUpdateConfig")
	}

	// Set in config map
	e.PersistentConfig.VCServiceToken = updateCfg.VCServiceToken
	e.PersistentConfig.GitHubToken = updateCfg.GithubToken

	e.log.Debugf("updated config via MessageUpdateConfig")

	return nil
}

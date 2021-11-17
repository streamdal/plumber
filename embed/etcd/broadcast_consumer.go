package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/server/types"
)

func (e *Etcd) handleBroadcastWatchResponse(ctx context.Context, resp *clientv3.WatchResponse) error {
	if resp == nil {
		return errors.New("response cannot be nil")
	}

	for _, v := range resp.Events {
		// We only care about creations
		if v.Type != clientv3.EventTypePut {
			continue
		}

		msg := &Message{}

		if err := json.Unmarshal(v.Kv.Value, msg); err != nil {
			e.log.Errorf("unable to unmarshal etcd key '%s' to message: %s", string(v.Kv.Key), err)
			continue
		}

		var err error

		// Add actions here that the consumer should respond to
		switch msg.Action {
		case CreateConnection:
			err = e.doCreateConnection(ctx, msg)
		case UpdateConnection:
			err = e.doUpdateConnection(ctx, msg)
		case DeleteConnection:
			err = e.doDeleteConnection(ctx, msg)
		case CreateService:
			err = e.doCreateService(ctx, msg)
		case UpdateService:
			err = e.doUpdateService(ctx, msg)
		case DeleteService:
			err = e.doDeleteService(ctx, msg)
		case CreateSchema:
			err = e.doCreateSchema(ctx, msg)
		case UpdateSchema:
			err = e.doUpdateSchema(ctx, msg)
		case DeleteSchema:
			err = e.doDeleteSchema(ctx, msg)
		case CreateRelay:
			err = e.doCreateRelay(ctx, msg)
		case UpdateRelay:
			err = e.doUpdateRelay(ctx, msg)
		case DeleteRelay:
			err = e.doDeleteRelay(ctx, msg)
		case UpdateConfig:
			err = e.doUpdateConfig(ctx, msg)
		case CreateValidation:
			err = e.doCreateValidation(ctx, msg)
		case UpdateValidation:
			err = e.doUpdateValidation(ctx, msg)
		case DeleteValidation:
			err = e.doDeleteValidation(ctx, msg)
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
	e.PlumberConfig.SetConnection(connOpts.XId, connOpts)

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
	e.PlumberConfig.SetConnection(connOpts.XId, connOpts)

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
	e.PlumberConfig.DeleteConnection(connOpts.XId)

	e.log.Debugf("deleted connection '%s'", connOpts.Name)

	// TODO: stop reads/relays from this connection?

	return nil
}

func (e *Etcd) doCreateService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	e.PlumberConfig.SetService(svc.Id, svc)

	e.log.Debugf("updated service '%s'", svc.Name)

	return nil
}

func (e *Etcd) doUpdateService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	e.PlumberConfig.SetService(svc.Id, svc)

	e.log.Debugf("updated service '%s'", svc.Name)

	return nil
}

func (e *Etcd) doDeleteService(_ context.Context, msg *Message) error {
	svc := &protos.Service{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Service")
	}

	// Set in config map
	e.PlumberConfig.DeleteService(svc.Id)

	e.log.Debugf("deleted service '%s'", svc.Name)

	return nil
}

func (e *Etcd) doCreateSchema(_ context.Context, msg *Message) error {
	schema := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, schema); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	e.PlumberConfig.SetSchema(schema.Id, schema)

	e.log.Debugf("updated schema '%s'", schema.Name)

	return nil
}

func (e *Etcd) doUpdateSchema(_ context.Context, msg *Message) error {
	schema := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, schema); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	e.PlumberConfig.SetSchema(schema.Id, schema)

	e.log.Debugf("updated schema '%s'", schema.Name)

	return nil
}

func (e *Etcd) doDeleteSchema(_ context.Context, msg *Message) error {
	svc := &protos.Schema{}
	if err := proto.Unmarshal(msg.Data, svc); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Schema")
	}

	// Set in config map
	e.PlumberConfig.DeleteSchema(svc.Id)

	e.log.Debugf("deleted schema '%s'", svc.Name)

	return nil
}

func (e *Etcd) doCreateRelay(_ context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Relay")
	}

	// Set in config map
	e.PlumberConfig.SetRelay(relayOptions.XRelayId, &types.Relay{Options: relayOptions})

	e.log.Debugf("created relay '%s'", relayOptions.XRelayId)

	return nil
}

func (e *Etcd) doUpdateRelay(_ context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Relay")
	}

	// Set in config map
	e.PlumberConfig.SetRelay(relayOptions.XRelayId, &types.Relay{Options: relayOptions})

	e.log.Debugf("updated relay '%s'", relayOptions.XRelayId)

	return nil
}

func (e *Etcd) doDeleteRelay(_ context.Context, msg *Message) error {
	relayOptions := &opts.RelayOptions{}
	if err := proto.Unmarshal(msg.Data, relayOptions); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into protos.Relay")
	}

	// Set in config map
	e.PlumberConfig.DeleteRelay(relayOptions.XRelayId)

	e.log.Debugf("deleted relay '%s'", relayOptions.XRelayId)

	return nil
}

func (e *Etcd) doCreateValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	e.PlumberConfig.SetValidation(validation.XId, validation)

	e.log.Debugf("created validation '%s'", validation.XId)

	return nil
}

func (e *Etcd) doUpdateValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	e.PlumberConfig.SetValidation(validation.XId, validation)

	e.log.Debugf("updated validation '%s'", validation.XId)

	return nil
}

func (e *Etcd) doDeleteValidation(_ context.Context, msg *Message) error {
	validation := &common.Validation{}
	if err := proto.Unmarshal(msg.Data, validation); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into common.Validation")
	}

	// Set in config map
	e.PlumberConfig.DeleteValidation(validation.XId)

	e.log.Debugf("deleted validation '%s'", validation.XId)

	return nil
}

func (e *Etcd) doCreateRead(_ context.Context, msg *Message) error {
	read := &opts.ReadOptions{}
	if err := proto.Unmarshal(msg.Data, read); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ReadOptions")
	}

	var md *desc.MessageDescriptor

	if err := e.populateDecodeSchemaDetails(read); err != nil {
		return fmt.Errorf("unable to create read '%s' from cache: %s", read.XId, err)
	}

	// TODO: can we move this elsewhere?
	if read.DecodeOptions != nil && read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		var mdErr error

		pbSettings := read.DecodeOptions.ProtobufSettings

		md, mdErr = pb.GetMDFromDescriptorBlob(pbSettings.XMessageDescriptor, pbSettings.ProtobufRootMessage)
		if mdErr != nil {
			return fmt.Errorf("unable to create read '%s' from cache: unable to generate protobuf message descriptor: %s", read.XId, mdErr)
		}
	}

	read.XActive = false

	ctx, cxl := context.WithCancel(context.Background())

	cfg := &types.Read{
		AttachedClientsMutex: &sync.RWMutex{},
		AttachedClients:      make(map[string]*types.AttachedStream),
		PlumberID:            e.PlumberConfig.PlumberID,
		ReadOptions:          read,
		ContextCxl:           ctx,
		CancelFunc:           cxl,
		Backend:              nil, // Will be filled in by StartRead()
		MsgDesc:              md,
		Log:                  logrus.WithField("read_id", read.XId),
	}

	// Set in config map
	e.PlumberConfig.SetRead(read.XId, cfg)

	e.log.Debugf("created read '%s'", read.XId)

	return nil
}

func (e *Etcd) doDeleteRead(_ context.Context, msg *Message) error {
	read := &opts.ReadOptions{}
	if err := proto.Unmarshal(msg.Data, read); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into opts.ReadOptions")
	}

	// Set in config map
	e.PlumberConfig.DeleteRead(read.XId)

	e.log.Debugf("deleted read '%s'", read.XId)

	return nil
}

func (e *Etcd) doUpdateConfig(_ context.Context, msg *Message) error {
	updateCfg := &MessageUpdateConfig{}
	if err := json.Unmarshal(msg.Data, updateCfg); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into MessageUpdateConfig")
	}

	// Set in config map
	e.PlumberConfig.VCServiceToken = updateCfg.VCServiceToken
	e.PlumberConfig.GitHubToken = updateCfg.GithubToken

	e.log.Debugf("updated config via MessageUpdateConfig")

	return nil
}

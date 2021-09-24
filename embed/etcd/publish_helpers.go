package etcd

import (
	"context"
	"encoding/json"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

// PublishCreateService publishes a CreateService message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateService(ctx context.Context, svc *protos.Service) error {
	return e.publishServiceMessage(ctx, CreateService, svc)
}

// PublishUpdateService publishes an UpdateService message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateService(ctx context.Context, svc *protos.Service) error {
	return e.publishServiceMessage(ctx, UpdateService, svc)
}

// PublishDeleteService publishes a DeleteService message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteService(ctx context.Context, svc *protos.Service) error {
	return e.publishServiceMessage(ctx, DeleteService, svc)
}

// PublishCreateConnection publishes a CreateConnection message, which other plumber instances will receive
// and add the connection to their local in-memory maps
func (e *Etcd) PublishCreateConnection(ctx context.Context, conn *opts.ConnectionOptions) error {
	return e.publishConnectionMessage(ctx, CreateConnection, conn)
}

// PublishUpdateConnection publishes an UpdateConnection message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateConnection(ctx context.Context, conn *opts.ConnectionOptions) error {
	return e.publishConnectionMessage(ctx, UpdateConnection, conn)
}

// PublishDeleteConnection publishes a DeleteConnection message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteConnection(ctx context.Context, conn *opts.ConnectionOptions) error {
	return e.publishConnectionMessage(ctx, DeleteConnection, conn)
}

// PublishCreateSchema publishes a CreateSchema message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateSchema(ctx context.Context, schema *protos.Schema) error {
	return e.publishSchemaMessage(ctx, CreateSchema, schema)
}

// PublishUpdateSchema publishes an UpdateSchema message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateSchema(ctx context.Context, schema *protos.Schema) error {
	return e.publishSchemaMessage(ctx, UpdateSchema, schema)
}

// PublishDeleteSchema publishes a DeleteSchema message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteSchema(ctx context.Context, schema *protos.Schema) error {
	return e.publishSchemaMessage(ctx, DeleteSchema, schema)
}

// PublishCreateRelay publishes a CreateRelay message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return e.publishRelayMessage(ctx, CreateRelay, relay)
}

// PublishUpdateRelay publishes an UpdateRelay message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return e.publishRelayMessage(ctx, UpdateRelay, relay)
}

// PublishDeleteRelay publishes a DeleteRelay message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return e.publishRelayMessage(ctx, DeleteSchema, relay)
}

// PublishConfigUpdate publishes a MessageUpdateConfig message, which other plumber instances
// will receive and update their config with the new token
func (e *Etcd) PublishConfigUpdate(ctx context.Context, msg *MessageUpdateConfig) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "unable to publish config update message")
	}

	return e.Broadcast(ctx, &Message{
		Action:    UpdateConfig,
		Data:      data,
		EmittedBy: e.PlumberConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishServiceMessage(ctx context.Context, action Action, svc *protos.Service) error {
	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrapf(err, "unable to publish service message for '%s'", svc.Id)
	}

	// Publish delete
	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PlumberConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishConnectionMessage(ctx context.Context, action Action, conn *opts.ConnectionOptions) error {
	data, err := proto.Marshal(conn)
	if err != nil {
		return errors.Wrapf(err, "unable to publish connection message for '%s'", conn.XId)
	}

	// Publish delete
	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PlumberConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishSchemaMessage(ctx context.Context, action Action, svc *protos.Schema) error {
	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrapf(err, "unable to publish schema message for '%s'", svc.Id)
	}

	// Publish delete
	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PlumberConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishRelayMessage(ctx context.Context, action Action, relay *opts.RelayOptions) error {
	data, err := proto.Marshal(relay)
	if err != nil {
		return errors.Wrapf(err, "unable to publish relay message for '%s'", relay.XRelayId)
	}

	// Publish delete
	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PlumberConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

package etcd

import (
	"context"
	"encoding/json"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

var (
	ErrMissingConfigMessage  = errors.New("msg cannot be nil")
	ErrMissingGithubToken    = errors.New("GithubToken cannot be empty")
	ErrMissingVCServiceToken = errors.New("VCServiceToken cannot be empty")
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
	return e.publishRelayMessage(ctx, DeleteRelay, relay)
}

// PublishStopRelay broadcasts a StopRelay message which will cause all plumber
// instances to stop the relay and remove it from their in-memory cache.
func (e *Etcd) PublishStopRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return e.publishRelayMessage(ctx, StopRelay, relay)
}

// PublishResumeRelay broadcasts a ResumeRelay message which will cause all plumber
// instances to start a stopped relay and add it to their in-memory cache.
func (e *Etcd) PublishResumeRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return e.publishRelayMessage(ctx, ResumeRelay, relay)
}

// PublishCreateValidation publishes a CreateValidation message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateValidation(ctx context.Context, validation *common.Validation) error {
	return e.publishValidationMessage(ctx, CreateValidation, validation)
}

// PublishUpdateValidation publishes an UpdateValidation message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateValidation(ctx context.Context, validation *common.Validation) error {
	return e.publishValidationMessage(ctx, UpdateValidation, validation)
}

// PublishDeleteValidation publishes a DeleteValidation message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteValidation(ctx context.Context, validation *common.Validation) error {
	return e.publishValidationMessage(ctx, DeleteSchema, validation)
}

// PublishCreateRead publishes a CreateRead message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateRead(ctx context.Context, read *opts.ReadOptions) error {
	return e.publishReadMessage(ctx, CreateRead, read)
}

// PublishDeleteRead publishes a DeleteRead message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteRead(ctx context.Context, read *opts.ReadOptions) error {
	return e.publishReadMessage(ctx, DeleteRead, read)
}

// PublishCreateComposite publishes a CreateComposite message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateComposite(ctx context.Context, comp *opts.Composite) error {
	return e.publishCompositeMessage(ctx, CreateComposite, comp)
}

// PublishUpdateComposite publishes an UpdateComposite message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateComposite(ctx context.Context, comp *opts.Composite) error {
	return e.publishCompositeMessage(ctx, UpdateComposite, comp)
}

// PublishDeleteComposite publishes a DeleteComposite message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteComposite(ctx context.Context, comp *opts.Composite) error {
	return e.publishCompositeMessage(ctx, DeleteSchema, comp)
}

// PublishCreateDynamic publishes a CreateDynamic message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (e *Etcd) PublishCreateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return e.publishDynamicMessage(ctx, CreateDynamic, dynamicOptions)
}

// PublishUpdateDynamic publishes an UpdateDynamic message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (e *Etcd) PublishUpdateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return e.publishDynamicMessage(ctx, UpdateDynamic, dynamicOptions)
}

// PublishDeleteDynamic publishes a DeleteDynamic message, which other plumber instances will receive
// and delete from their local in-memory maps
func (e *Etcd) PublishDeleteDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return e.publishDynamicMessage(ctx, DeleteDynamic, dynamicOptions)
}

// PublishStopDynamic broadcasts a StopDynamic message which will cause all plumber
// instances to stop the relay and remove it from their in-memory cache.
func (e *Etcd) PublishStopDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return e.publishDynamicMessage(ctx, StopDynamic, dynamicOptions)
}

// PublishResumeDynamic broadcasts a ResumeDynamic message which will cause all plumber
// instances to start a stopped relay and add it to their in-memory cache.
func (e *Etcd) PublishResumeDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return e.publishDynamicMessage(ctx, ResumeDynamic, dynamicOptions)
}

// PublishConfigUpdate publishes a MessageUpdateConfig message, which other plumber instances
// will receive and update their config with the new token
func (e *Etcd) PublishConfigUpdate(ctx context.Context, msg *MessageUpdateConfig) error {
	return nil // TODO: remove

	if err := validateMessageUpdateConfig(msg); err != nil {
		return errors.Wrap(err, "unable to validate MessageUpdateConfig")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "unable to publish config update message")
	}

	return e.Broadcast(ctx, &Message{
		Action:    UpdateConfig,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func validateMessageUpdateConfig(msg *MessageUpdateConfig) error {
	if msg == nil {
		return ErrMissingConfigMessage
	}

	if msg.GithubToken == "" {
		return ErrMissingGithubToken
	}

	if msg.VCServiceToken == "" {
		return ErrMissingVCServiceToken
	}

	return nil
}

func (e *Etcd) publishServiceMessage(ctx context.Context, action Action, svc *protos.Service) error {
	return nil // TODO: remove

	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrapf(err, "unable to publish service message for '%s'", svc.Id)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishConnectionMessage(ctx context.Context, action Action, conn *opts.ConnectionOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(conn)
	if err != nil {
		return errors.Wrapf(err, "unable to publish connection message for '%s'", conn.XId)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishSchemaMessage(ctx context.Context, action Action, svc *protos.Schema) error {
	return nil // TODO: remove

	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrapf(err, "unable to publish schema message for '%s'", svc.Id)
	}

	// Publish delete
	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishRelayMessage(ctx context.Context, action Action, relay *opts.RelayOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(relay)
	if err != nil {
		return errors.Wrapf(err, "unable to publish relay message for '%s'", relay.XRelayId)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishValidationMessage(ctx context.Context, action Action, validation *common.Validation) error {
	return nil // TODO: remove

	data, err := proto.Marshal(validation)
	if err != nil {
		return errors.Wrapf(err, "unable to publish validation message for '%s'", validation.XId)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishReadMessage(ctx context.Context, action Action, read *opts.ReadOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(read)
	if err != nil {
		return errors.Wrapf(err, "unable to publish read message for '%s'", read.XId)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishCompositeMessage(ctx context.Context, action Action, composite *opts.Composite) error {
	return nil // TODO: remove

	data, err := proto.Marshal(composite)
	if err != nil {
		return errors.Wrapf(err, "unable to publish composite message for '%s'", composite.XId)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (e *Etcd) publishDynamicMessage(ctx context.Context, action Action, dynamicOptions *opts.DynamicOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(dynamicOptions)
	if err != nil {
		return errors.Wrapf(err, "unable to publish dynamic message for '%s'", dynamicOptions.XDynamicId)
	}

	return e.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: e.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

package bus

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
func (b *Bus) PublishCreateService(ctx context.Context, svc *protos.Service) error {
	return b.publishServiceMessage(ctx, CreateService, svc)
}

// PublishUpdateService publishes an UpdateService message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateService(ctx context.Context, svc *protos.Service) error {
	return b.publishServiceMessage(ctx, UpdateService, svc)
}

// PublishDeleteService publishes a DeleteService message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteService(ctx context.Context, svc *protos.Service) error {
	return b.publishServiceMessage(ctx, DeleteService, svc)
}

// PublishCreateConnection publishes a CreateConnection message, which other plumber instances will receive
// and add the connection to their local in-memory maps
func (b *Bus) PublishCreateConnection(ctx context.Context, conn *opts.ConnectionOptions) error {
	return b.publishConnectionMessage(ctx, CreateConnection, conn)
}

// PublishUpdateConnection publishes an UpdateConnection message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateConnection(ctx context.Context, conn *opts.ConnectionOptions) error {
	return b.publishConnectionMessage(ctx, UpdateConnection, conn)
}

// PublishDeleteConnection publishes a DeleteConnection message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteConnection(ctx context.Context, conn *opts.ConnectionOptions) error {
	return b.publishConnectionMessage(ctx, DeleteConnection, conn)
}

// PublishCreateSchema publishes a CreateSchema message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (b *Bus) PublishCreateSchema(ctx context.Context, schema *protos.Schema) error {
	return b.publishSchemaMessage(ctx, CreateSchema, schema)
}

// PublishUpdateSchema publishes an UpdateSchema message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateSchema(ctx context.Context, schema *protos.Schema) error {
	return b.publishSchemaMessage(ctx, UpdateSchema, schema)
}

// PublishDeleteSchema publishes a DeleteSchema message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteSchema(ctx context.Context, schema *protos.Schema) error {
	return b.publishSchemaMessage(ctx, DeleteSchema, schema)
}

// PublishCreateRelay publishes a CreateRelay message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (b *Bus) PublishCreateRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return b.publishRelayMessage(ctx, CreateRelay, relay)
}

// PublishUpdateRelay publishes an UpdateRelay message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return b.publishRelayMessage(ctx, UpdateRelay, relay)
}

// PublishDeleteRelay publishes a DeleteRelay message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return b.publishRelayMessage(ctx, DeleteRelay, relay)
}

// PublishStopRelay broadcasts a StopRelay message which will cause all plumber
// instances to stop the relay and remove it from their in-memory cache.
func (b *Bus) PublishStopRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return b.publishRelayMessage(ctx, StopRelay, relay)
}

// PublishResumeRelay broadcasts a ResumeRelay message which will cause all plumber
// instances to start a stopped relay and add it to their in-memory cache.
func (b *Bus) PublishResumeRelay(ctx context.Context, relay *opts.RelayOptions) error {
	return b.publishRelayMessage(ctx, ResumeRelay, relay)
}

// PublishCreateValidation publishes a CreateValidation message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (b *Bus) PublishCreateValidation(ctx context.Context, validation *common.Validation) error {
	return b.publishValidationMessage(ctx, CreateValidation, validation)
}

// PublishUpdateValidation publishes an UpdateValidation message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateValidation(ctx context.Context, validation *common.Validation) error {
	return b.publishValidationMessage(ctx, UpdateValidation, validation)
}

// PublishDeleteValidation publishes a DeleteValidation message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteValidation(ctx context.Context, validation *common.Validation) error {
	return b.publishValidationMessage(ctx, DeleteSchema, validation)
}

// PublishCreateRead publishes a CreateRead message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (b *Bus) PublishCreateRead(ctx context.Context, read *opts.ReadOptions) error {
	return b.publishReadMessage(ctx, CreateRead, read)
}

// PublishDeleteRead publishes a DeleteRead message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteRead(ctx context.Context, read *opts.ReadOptions) error {
	return b.publishReadMessage(ctx, DeleteRead, read)
}

// PublishCreateComposite publishes a CreateComposite message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (b *Bus) PublishCreateComposite(ctx context.Context, comp *opts.Composite) error {
	return b.publishCompositeMessage(ctx, CreateComposite, comp)
}

// PublishUpdateComposite publishes an UpdateComposite message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateComposite(ctx context.Context, comp *opts.Composite) error {
	return b.publishCompositeMessage(ctx, UpdateComposite, comp)
}

// PublishDeleteComposite publishes a DeleteComposite message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteComposite(ctx context.Context, comp *opts.Composite) error {
	return b.publishCompositeMessage(ctx, DeleteSchema, comp)
}

// PublishCreateDynamic publishes a CreateDynamic message, which other plumber instances will receive
// and add the service to their local in-memory maps
func (b *Bus) PublishCreateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return b.publishDynamicMessage(ctx, CreateDynamic, dynamicOptions)
}

// PublishUpdateDynamic publishes an UpdateDynamic message, which other plumber instances will receive
// and update the connection in their local in-memory maps
func (b *Bus) PublishUpdateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return b.publishDynamicMessage(ctx, UpdateDynamic, dynamicOptions)
}

// PublishDeleteDynamic publishes a DeleteDynamic message, which other plumber instances will receive
// and delete from their local in-memory maps
func (b *Bus) PublishDeleteDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return b.publishDynamicMessage(ctx, DeleteDynamic, dynamicOptions)
}

// PublishStopDynamic broadcasts a StopDynamic message which will cause all plumber
// instances to stop the relay and remove it from their in-memory cache.
func (b *Bus) PublishStopDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return b.publishDynamicMessage(ctx, StopDynamic, dynamicOptions)
}

// PublishResumeDynamic broadcasts a ResumeDynamic message which will cause all plumber
// instances to start a stopped relay and add it to their in-memory cache.
func (b *Bus) PublishResumeDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error {
	return b.publishDynamicMessage(ctx, ResumeDynamic, dynamicOptions)
}

// PublishConfigUpdate publishes a MessageUpdateConfig message, which other plumber instances
// will receive and update their config with the new token
func (b *Bus) PublishConfigUpdate(ctx context.Context, msg *MessageUpdateConfig) error {
	if err := validateMessageUpdateConfig(msg); err != nil {
		return errors.Wrap(err, "unable to validate MessageUpdateConfig")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "unable to publish config update message")
	}

	return b.Broadcast(ctx, &Message{
		Action:    UpdateConfig,
		Data:      data,
		EmittedBy: b.config.PlumberID,
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

func (b *Bus) publishServiceMessage(ctx context.Context, action Action, svc *protos.Service) error {
	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrapf(err, "unable to publish service message for '%s'", svc.Id)
	}

	return b.broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishConnectionMessage(ctx context.Context, action Action, conn *opts.ConnectionOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(conn)
	if err != nil {
		return errors.Wrapf(err, "unable to publish connection message for '%s'", conn.XId)
	}

	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishSchemaMessage(ctx context.Context, action Action, svc *protos.Schema) error {
	return nil // TODO: remove

	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrapf(err, "unable to publish schema message for '%s'", svc.Id)
	}

	// Publish delete
	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishRelayMessage(ctx context.Context, action Action, relay *opts.RelayOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(relay)
	if err != nil {
		return errors.Wrapf(err, "unable to publish relay message for '%s'", relay.XRelayId)
	}

	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishValidationMessage(ctx context.Context, action Action, validation *common.Validation) error {
	return nil // TODO: remove

	data, err := proto.Marshal(validation)
	if err != nil {
		return errors.Wrapf(err, "unable to publish validation message for '%s'", validation.XId)
	}

	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishReadMessage(ctx context.Context, action Action, read *opts.ReadOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(read)
	if err != nil {
		return errors.Wrapf(err, "unable to publish read message for '%s'", read.XId)
	}

	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishCompositeMessage(ctx context.Context, action Action, composite *opts.Composite) error {
	return nil // TODO: remove

	data, err := proto.Marshal(composite)
	if err != nil {
		return errors.Wrapf(err, "unable to publish composite message for '%s'", composite.XId)
	}

	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

func (b *Bus) publishDynamicMessage(ctx context.Context, action Action, dynamicOptions *opts.DynamicOptions) error {
	return nil // TODO: remove

	data, err := proto.Marshal(dynamicOptions)
	if err != nil {
		return errors.Wrapf(err, "unable to publish dynamic message for '%s'", dynamicOptions.XDynamicId)
	}

	return b.Broadcast(ctx, &Message{
		Action:    action,
		Data:      data,
		EmittedBy: b.PersistentConfig.PlumberID,
		EmittedAt: time.Now().UTC(),
	})
}

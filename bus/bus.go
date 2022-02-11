package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/batchcorp/natty"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/actions"
	"github.com/batchcorp/plumber/config"
)

const (
	StreamName       = "events"
	BroadcastSubject = "broadcast"
	QueueSubject     = "queue"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IEtcd
type IBus interface {
	// Start starts up the broadcast and queue consumers
	Start(serviceCtx context.Context) error

	// Stop stops the broadcast and queue consumers
	Stop() error

	// Convenience methods for publishing messages on the bus (without having
	// to know if they are intended to be broadcast or queue messages)

	PublishCreateService(ctx context.Context, svc *protos.Service) error
	PublishUpdateService(ctx context.Context, svc *protos.Service) error
	PublishDeleteService(ctx context.Context, svc *protos.Service) error

	PublishCreateConnection(ctx context.Context, conn *opts.ConnectionOptions) error
	PublishUpdateConnection(ctx context.Context, conn *opts.ConnectionOptions) error
	PublishDeleteConnection(ctx context.Context, conn *opts.ConnectionOptions) error

	PublishCreateSchema(ctx context.Context, schema *protos.Schema) error
	PublishUpdateSchema(ctx context.Context, schema *protos.Schema) error
	PublishDeleteSchema(ctx context.Context, schema *protos.Schema) error

	PublishCreateRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishUpdateRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishDeleteRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishStopRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishResumeRelay(ctx context.Context, relay *opts.RelayOptions) error

	PublishConfigUpdate(ctx context.Context, msg *MessageUpdateConfig) error

	PublishCreateValidation(ctx context.Context, validation *common.Validation) error
	PublishUpdateValidation(ctx context.Context, validation *common.Validation) error
	PublishDeleteValidation(ctx context.Context, validation *common.Validation) error

	PublishCreateRead(ctx context.Context, svc *opts.ReadOptions) error
	PublishDeleteRead(ctx context.Context, svc *opts.ReadOptions) error

	PublishCreateComposite(ctx context.Context, validation *opts.Composite) error
	PublishUpdateComposite(ctx context.Context, validation *opts.Composite) error
	PublishDeleteComposite(ctx context.Context, validation *opts.Composite) error

	PublishCreateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error
	PublishUpdateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error
	PublishStopDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error
	PublishResumeDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error
	PublishDeleteDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) error
}

type Bus struct {
	config              *Config
	serverOptions       *opts.ServerOptions
	broadcastClient     natty.INatty
	queueClient         natty.INatty
	consumerErrChan     chan error
	consumerContext     context.Context
	consumerCancelFunc  context.CancelFunc
	consumerErrorLooper director.Looper
	running             bool
	log                 *logrus.Entry
}

type Config struct {
	ServerOptions    *opts.ServerOptions
	PersistentConfig *config.Config // Consumers will need this to update their local config
	Actions          *actions.Actions
}

var (
	ConsumersNotRunning     = errors.New("nothing to stop - consumers not running")
	ConsumersAlreadyRunning = errors.New("cannot start - consumers already running")
)

func New(cfg *Config) (*Bus, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate bus config")
	}

	b := &Bus{
		config:              cfg,
		consumerErrChan:     make(chan error, 1000),
		consumerErrorLooper: director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		log: logrus.WithFields(logrus.Fields{
			"component": "bus",
		}),
	}

	if err := b.setupClients(); err != nil {
		return nil, errors.Wrap(err, "unable to setup natty clients")
	}

	return b, nil
}

// broadcast() is a helper wrapper for Publish()
func (b *Bus) broadcast(ctx context.Context, msg *Message) error {
	return b.publish(ctx, StreamName+"."+BroadcastSubject, msg)
}

// queue() is a helper wrapper for Publish()
func (b *Bus) queue(ctx context.Context, msg *Message) error {
	return b.publish(ctx, StreamName+"."+QueueSubject, msg)
}

func (b *Bus) publish(ctx context.Context, subject string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "unable to marshal message")
	}

	return b.broadcastClient.Publish(ctx, subject, data)

}

func (b *Bus) Start(serviceCtx context.Context) error {
	if b.running {
		return ConsumersAlreadyRunning
	}

	broadcastErrChan := make(chan error, 1)
	queueErrChan := make(chan error, 1)

	// Used for starting and stopping consumers
	consumerCtx, cancelFunc := context.WithCancel(context.Background())

	b.consumerCancelFunc = cancelFunc
	b.consumerContext = consumerCtx

	b.log.Debug("starting broadcast consumer")

	// Start broadcast consumer
	go func() {
		if err := b.runBroadcastConsumer(consumerCtx); err != nil {
			b.log.Warningf("broadcast consumer exit due to err: %s", err)
			broadcastErrChan <- err
		}
	}()

	b.log.Debug("starting queue consumer")

	// Start queue consumer
	go func() {
		if err := b.runQueueConsumer(consumerCtx); err != nil {
			b.log.Warningf("queue consumer exit due to err: %s", err)
			queueErrChan <- err
		}
	}()

	b.log.Debug("starting error watcher")

	// Launch consumer error watcher
	go func() {
		if err := b.runConsumerErrorWatcher(serviceCtx, consumerCtx); err != nil {
			b.log.Warningf("consumer error watcher exit: %s", err)
		}
	}()

	// Listen for errors for a bit
	timerCh := time.After(10 * time.Second)

	select {
	case <-timerCh:
		break
	case err := <-broadcastErrChan:
		cancelFunc()
		return errors.Wrap(err, "error running broadcast consumer")
	case err := <-queueErrChan:
		cancelFunc()
		return errors.Wrap(err, "error running queue consumer")
	}

	b.log.Debug("starting shutdown listener")

	go b.runServiceShutdownListener(serviceCtx)

	b.running = true

	return nil
}

func (b *Bus) runConsumerErrorWatcher(serviceCtx context.Context, consumerCtx context.Context) error {
	var quit bool

	b.consumerErrorLooper.Loop(func() error {
		if quit {
			// Give looper a moment to catch up
			time.Sleep(1 * time.Second)
			return nil
		}

		select {
		case err := <-b.consumerErrChan:
			b.log.Errorf("consumer error: %s", err)
		case <-serviceCtx.Done():
			b.log.Warning("service shutdown detected, stopping consumer error watcher")
			b.consumerErrorLooper.Quit()
			quit = true
		case <-consumerCtx.Done():
			b.log.Warning("service shutdown detected, stopping consumer error watcher")
			b.consumerErrorLooper.Quit()
			quit = true
		}

		return nil
	})

	b.log.Debug("consumer error watcher exiting")

	return nil
}

func (b *Bus) setupClients() error {
	// Setup broadcast client
	nattyCfg := natty.Config{
		NatsURL:        b.config.ServerOptions.NatsUrl,
		StreamName:     StreamName,
		StreamSubjects: []string{StreamName + ".*"},
		MaxMsgs:        1000,
		FetchSize:      1,
		FetchTimeout:   time.Second * 1,
		DeliverPolicy:  nats.DeliverNewPolicy,
		Logger:         b.log,
	}

	if b.config.ServerOptions.UseTls {
		nattyCfg.UseTLS = true
		nattyCfg.TLSSkipVerify = b.config.ServerOptions.TlsSkipVerify
		nattyCfg.TLSClientCertFile = b.config.ServerOptions.TlsCertFile
		nattyCfg.TLSClientKeyFile = b.config.ServerOptions.TlsKeyFile
		nattyCfg.TLSCACertFile = b.config.ServerOptions.TlsCaFile
	}

	broadcastCfg := nattyCfg

	// This is the important part - since every consumer will have a unique
	// consumer name, NATS will send every plumber instance a copy of the message
	broadcastCfg.ConsumerName = b.config.ServerOptions.NodeId

	broadcastClient, err := natty.New(&broadcastCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create broadcast client")
	}

	queueCfg := nattyCfg

	// By assigning a common consumer name, NATS will deliver the message to
	// only one plumber instance.
	broadcastCfg.ConsumerName = "queue-consumer"

	// Setup queue client
	queueClient, err := natty.New(&queueCfg)

	if err != nil {
		return errors.Wrap(err, "unable to create queue client")
	}

	b.queueClient = queueClient
	b.broadcastClient = broadcastClient

	return nil
}

func (b *Bus) runServiceShutdownListener(serviceCtx context.Context) {
MAIN:
	for {
		select {
		case <-serviceCtx.Done():
			b.log.Debug("bus consumers caught service exit request")
			break MAIN
		}
	}

	if err := b.Stop(); err != nil {
		b.log.Errorf("unable to shutdown consumers: %s", err)
	}
}

func (b *Bus) Stop() error {
	if !b.running {
		return ConsumersNotRunning
	}

	b.consumerCancelFunc() // Should cause consumer goroutines to exit

	// Give some time for consumers to exit
	time.Sleep(5 * time.Second)

	return nil
}

func (b *Bus) runBroadcastConsumer(consumerCtx context.Context) error {
	subject := StreamName + "." + BroadcastSubject

	llog := b.log.WithFields(logrus.Fields{
		"consumer": "broadcast",
		"subject":  subject,
	})

	for {
		err := b.broadcastClient.Consume(consumerCtx, subject, b.consumerErrChan, b.broadcastCallback)
		if err != nil {
			if err == context.Canceled {
				llog.Debug("broadcast consumer context cancelled")
				break
			}
		}
	}

	llog.Debug("exiting")

	return nil
}

func (b *Bus) runQueueConsumer(consumerCtx context.Context) error {
	return nil
}

// TODO: Where should this get called from?
// PopulateCache loads config from etcd
func (b *Bus) PopulateCache() error {
	if err := b.populateServerConfigCache(); err != nil {
		return errors.Wrap(err, "unable to populate server configs from cache")
	}

	if err := b.populateConnectionCache(); err != nil {
		return errors.Wrap(err, "unable to populate connection configs from cache")
	}

	if err := b.populateServiceCache(); err != nil {
		return errors.Wrap(err, "unable to populate service configs from cache")
	}

	if err := b.populateSchemaCache(); err != nil {
		return errors.Wrap(err, "unable to populate schema configs from cache")
	}

	if err := b.populateRelayCache(); err != nil {
		return errors.Wrap(err, "unable to populate relay configs from cache")
	}

	if err := b.populateValidationCache(); err != nil {
		return errors.Wrap(err, "unable to populate schema validation configs from cache")
	}

	if err := b.populateReadCache(); err != nil {
		return errors.Wrap(err, "unable to populate read configs from cache")
	}

	return nil
}

func (b *Bus) populateServerConfigCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheServerConfigKey)
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch server config from etcd")
	//}
	//
	//// Nothing stored yet, do nothing
	//if len(resp.Kvs) == 0 {
	//	return nil
	//}
	//
	//cfg := &config.Config{}
	//if err := json.Unmarshal(resp.Kvs[0].Value, cfg); err != nil {
	//	return errors.Wrap(err, "unable to unmarshal cached server config")
	//}
	//
	//// These config values on the ones saved in etcd
	//b.PersistentConfig.VCServiceToken = cfg.VCServiceToken
	//b.PersistentConfig.GitHubToken = cfg.GitHubToken
	//b.PersistentConfig.GitHubInstallID = cfg.GitHubInstallID
	//
	//// These values are the ones saved in config.json
	//
	//// TODO: Can we have some migration path from config.json to etcd? Regular plumber mode doesn't launch
	//// TODO: embedded etcd, so that needs to be handled somehow
	//
	//// Wrapped in if block in case config is still stored in config.json
	//if b.PersistentConfig.UserID == "" {
	//	b.PersistentConfig.UserID = cfg.UserID
	//}
	//
	//// Wrapped in if block in case config is still stored in config.json
	//if b.PersistentConfig.PlumberID == "" {
	//	b.PersistentConfig.PlumberID = cfg.PlumberID
	//}
	//
	//// Wrapped in if block in case config is still stored in config.json
	//if b.PersistentConfig.TeamID == "" {
	//	b.PersistentConfig.TeamID = cfg.TeamID
	//}
	//
	//// Wrapped in if block in case config is still stored in config.json
	//if b.PersistentConfig.Token == "" {
	//	b.PersistentConfig.Token = cfg.Token
	//}
	//
	//return nil
}

func (b *Bus) populateConnectionCache() error {
	//return nil
	//
	//resp, err := b.Get(context.Background(), CacheConnectionsPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch protos.Connection messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	conn := &opts.ConnectionOptions{}
	//	if err := proto.Unmarshal(v.Value, conn); err != nil {
	//		b.log.Errorf("unable to unmarshal protos.Connection message: %s", err)
	//		continue
	//	}
	//
	//	count++
	//
	//	b.PersistentConfig.SetConnection(conn.XId, conn
	//}
	//
	//b.log.Debugf("Loaded '%d' connections from etcd", count)

	return nil
}

func (b *Bus) populateSchemaCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheSchemasPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch protos.Schema messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	schema := &protos.Schema{}
	//	if err := proto.Unmarshal(v.Value, schema); err != nil {
	//		b.log.Errorf("unable to unmarshal protos.Schema message: %s", err)
	//		continue
	//	}
	//
	//	count++
	//
	//	b.PersistentConfig.SetSchema(schema.Id, schema)
	//}
	//
	//b.log.Debugf("Loaded '%d' schemas from etcd", count)
	//
	//return nil
}

func (b *Bus) populateRelayCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheRelaysPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch opts.RelayOptions messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	relayOptions := &opts.RelayOptions{}
	//	if err := proto.Unmarshal(v.Value, relayOptions); err != nil {
	//		b.log.Errorf("unable to unmarshal opts.RelayOptions message: %s", err)
	//		continue
	//	}
	//
	//	if _, err := b.Actions.CreateRelay(context.Background(), relayOptions); err != nil {
	//		b.log.Errorf("unable to create relay for '%s': %s", relayOptions.XRelayId, err)
	//		continue
	//	}
	//
	//	count++
	//}
	//
	//b.log.Infof("Created '%d' relays from etcd", count)
	//
	//return nil
}

func (b *Bus) populateServiceCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheServicesPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch protos.Service messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	svc := &protos.Service{}
	//	if err := proto.Unmarshal(v.Value, svc); err != nil {
	//		b.log.Errorf("unable to unmarshal protos.Service message: %s", err)
	//		continue
	//	}
	//
	//	count++
	//
	//	b.PersistentConfig.SetService(svc.Id, svc)
	//}
	//
	//b.log.Debugf("Loaded '%d' services from etcd", count)
	//
	//return nil
}

func (b *Bus) populateValidationCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheValidationsPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch protos.Service messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	validation := &common.Validation{}
	//	if err := proto.Unmarshal(v.Value, validation); err != nil {
	//		b.log.Errorf("unable to unmarshal protos.Validation message: %s", err)
	//		continue
	//	}
	//
	//	count++
	//
	//	b.PersistentConfig.SetValidation(validation.XId, validation)
	//}
	//
	//b.log.Debugf("Loaded '%d' schema validations from etcd", count)
	//
	//return nil
}

// populateReadCache loads cached read configs from etcd.
// This method MUST be called after populateConnectionCache() and populateSchemaCache()
func (b *Bus) populateReadCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheReadsPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch opts.ReadOptions messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	readOpts := &opts.ReadOptions{}
	//	if err := proto.Unmarshal(v.Value, readOpts); err != nil {
	//		b.log.Errorf("unable to unmarshal opts.ReadOptions message: %s", err)
	//		continue
	//	}
	//
	//	if err := b.populateDecodeSchemaDetails(readOpts); err != nil {
	//		b.log.Errorf("unable to create readOpts '%s' from cache: %s", readOpts.XId, err)
	//		continue
	//	}
	//
	//	readOpts.XActive = false
	//
	//	count++
	//
	//	read, err := types.NewRead(&types.ReadConfig{
	//		ReadOptions: readOpts,
	//		PlumberID:   b.PersistentConfig.PlumberID,
	//		Backend:     nil, // intentionally nil
	//	})
	//	if err != nil {
	//		return errors.Wrap(err, "cannot create new read")
	//	}
	//
	//	b.PersistentConfig.SetRead(readOpts.XId, read)
	//}
	//
	//b.log.Debugf("Loaded '%d' reads from etcd", count)
	//
	//return nil
}

func (b *Bus) populateCompositeCache() error {
	return nil

	//resp, err := b.Get(context.Background(), CacheCompositesPrefix, clientv3.WithPrefix())
	//if err != nil {
	//	return errors.Wrap(err, "unable to fetch opts.Composite messages from etcd")
	//}
	//
	//var count int
	//
	//for _, v := range resp.Kvs {
	//	comp := &opts.Composite{}
	//	if err := proto.Unmarshal(v.Value, comp); err != nil {
	//		b.log.Errorf("unable to unmarshal opts.Composite message: %s", err)
	//		continue
	//	}
	//
	//	count++
	//
	//	b.PersistentConfig.SetComposite(comp.XId, comp)
	//}
	//
	//b.log.Debugf("Loaded '%d' composite views from etcd", count)
	//
	//return nil
}

// TODO: this method is duplicated from server.go, can we combine and stick somewhere else to avoid duplication?
func (b *Bus) populateDecodeSchemaDetails(read *opts.ReadOptions) error {
	if read.DecodeOptions == nil {
		return nil
	}

	schemaID := read.DecodeOptions.SchemaId
	if schemaID == "" {
		return nil
	}

	cachedSchemaOptions := b.config.PersistentConfig.GetSchema(schemaID)
	if cachedSchemaOptions == nil {
		return fmt.Errorf("schema '%s' not found", schemaID)
	}

	versions := cachedSchemaOptions.GetVersions()
	latestSchema := versions[len(versions)-1]

	switch read.DecodeOptions.DecodeType {
	case encoding.DecodeType_DECODE_TYPE_PROTOBUF:
		// Set the entire struct, since it probably won't be passed if just a schema ID is passed
		read.DecodeOptions.ProtobufSettings = &encoding.ProtobufSettings{
			ProtobufRootMessage: latestSchema.GetProtobufSettings().ProtobufRootMessage,
			XMessageDescriptor:  latestSchema.GetProtobufSettings().XMessageDescriptor,
		}
	case encoding.DecodeType_DECODE_TYPE_AVRO:
		// Set the entire struct, since it probably won't be passed if just a schema ID is passed
		read.DecodeOptions.AvroSettings = &encoding.AvroSettings{
			AvroSchemaFile: latestSchema.GetAvroSettings().AvroSchemaFile,
			Schema:         latestSchema.GetAvroSettings().Schema,
		}
	case encoding.DecodeType_DECODE_TYPE_THRIFT:
		// TODO: implement eventually
	}

	return nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if cfg.PersistentConfig == nil {
		return errors.New("persistent config cannot be nil")
	}

	if cfg.ServerOptions == nil {
		return errors.New("server options cannot be nil")
	}

	if cfg.Actions == nil {
		return errors.New("actions cannot be nil")
	}

	return nil
}

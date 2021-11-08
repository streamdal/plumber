package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/server/types"
)

const (
	BroadcastPath          = "/bus/broadcast"
	QueuePath              = "/bus/queue"
	CacheConnectionsPrefix = "/plumber-server/connections"
	CacheSchemasPrefix     = "/plumber-server/schemas"
	CacheRelaysPrefix      = "/plumber-server/relay"
	CacheServicesPrefix    = "/plumber-server/services"
	CacheValidationsPrefix = "/plumber-server/validations"
	CacheServerConfigKey   = "/plumber-server/server-config"
)

type HandlerFunc func(context.Context, *clientv3.WatchResponse) error

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IEtcd
type IEtcd interface {
	// client methods

	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)

	// Server methods

	Broadcast(ctx context.Context, msg *Message) error
	Direct(ctx context.Context, node string, msg *Message) error
	SaveConfig(ctx context.Context, cfg *config.Config) error
	Shutdown(force bool) error
	Start(serviceCtx context.Context) error

	// Message Publish helpers

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
	PublishCreateValidation(ctx context.Context, validation *common.Validation) error
	PublishConfigUpdate(ctx context.Context, msg *MessageUpdateConfig) error
	PublishUpdateValidation(ctx context.Context, validation *common.Validation) error
	PublishDeleteValidation(ctx context.Context, validation *common.Validation) error

	Client() *clientv3.Client
}

type Etcd struct {
	PlumberConfig *config.Config

	server             *embed.Etcd
	client             *clientv3.Client
	serverOptions      *opts.ServerOptions
	urls               *urls
	started            bool
	consumerContext    context.Context
	consumerCancelFunc context.CancelFunc
	log                *logrus.Entry
}

// URLs are specified as string in proto schemas - this is an intermediate holder
type urls struct {
	AdvertiseClientURL *url.URL
	AdvertisePeerURL   *url.URL
	ListenerClientURL  *url.URL
	ListenerPeerURL    *url.URL
}

var (
	ServerNotStartedErr     = errors.New("server not started")
	ServerAlreadyStartedErr = errors.New("server already started")
)

func New(serverOptions *opts.ServerOptions, plumberConfig *config.Config) (*Etcd, error) {
	if err := validateOptions(serverOptions); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	urls, err := parseURLs(serverOptions)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse etcd URLs")
	}

	return &Etcd{
		serverOptions: serverOptions,
		urls:          urls,
		PlumberConfig: plumberConfig,
		log:           logrus.WithField("pkg", "etcd"),
	}, nil
}

func parseURLs(serverOptions *opts.ServerOptions) (*urls, error) {
	acu, err := url.Parse(serverOptions.AdvertiseClientUrl)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse advertise client URL")
	}

	apu, err := url.Parse(serverOptions.AdvertisePeerUrl)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse advertise peer URL")
	}

	lcu, err := url.Parse(serverOptions.ListenerClientUrl)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse advertise client URL")
	}

	lpu, err := url.Parse(serverOptions.ListenerPeerUrl)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse advertise peer URL")
	}

	return &urls{
		AdvertiseClientURL: acu,
		AdvertisePeerURL:   apu,
		ListenerClientURL:  lcu,
		ListenerPeerURL:    lpu,
	}, nil
}

func validateOptions(cfg *opts.ServerOptions) error {
	if cfg == nil {
		return errors.New("server options config cannot be nil")
	}

	if cfg.InitialCluster == "" {
		return errors.New("InitialCluster setting cannot be empty")
	}

	if cfg.AdvertisePeerUrl == "" {
		return errors.New("AdvertisePeerURL cannot be empty")
	}

	if cfg.AdvertiseClientUrl == "" {
		return errors.New("AdvertiseClientURL cannot be empty")
	}

	if cfg.ListenerPeerUrl == "" {
		return errors.New("ListenerPeerURL cannot be empty")
	}

	if cfg.ListenerClientUrl == "" {
		return errors.New("ListenerClientURL cannot be empty")
	}

	if cfg.PeerToken == "" {
		return errors.New("PeerToken cannot be nil")
	}

	var numPeers int

	numPeers = len(strings.Split(cfg.InitialCluster, ","))

	if numPeers%2 == 0 {
		return errors.New("must have an odd number of peers")
	}

	return nil
}

func (e *Etcd) Client() *clientv3.Client {
	return e.client
}

func (e *Etcd) Start(serviceCtx context.Context) error {
	if e.started {
		return ServerAlreadyStartedErr
	}

	broadcastErr := make(chan error, 1)
	directErr := make(chan error, 1)

	consumerCtx, cancelFunc := context.WithCancel(context.Background())

	e.consumerCancelFunc = cancelFunc
	e.consumerContext = consumerCtx

	embeddedEtcd, err := e.launchEmbeddedEtcd(serviceCtx)
	if err != nil {
		cancelFunc() // just here to avoid IDE yelling at me about un-cancelled func
		e.log.Warningf("embedded etcd exit due to err: %s", err)
		return errors.Wrap(err, "unable to launch embedded etcd")
	}

	// Setup etcd client
	client, err := e.createClient("127.0.0.1:2379")
	if err != nil {
		cancelFunc()
		return errors.Wrap(err, "unable to create etcd client")
	}

	e.server = embeddedEtcd
	e.client = client

	// Start broadcast consumer
	go func() {
		if err := e.runBroadcastConsumer(serviceCtx, consumerCtx); err != nil {
			e.log.Warningf("etcd broadcast consumer exit due to err: %s", err)

			broadcastErr <- err
		}
	}()

	// Start direct consumer
	go func() {
		if err := e.runDirectConsumer(serviceCtx, consumerCtx); err != nil {
			e.log.Warningf("etcd direct consumer exit due to err: %s", err)
			directErr <- err
		}
	}()

	// Listen for errors for a bit
	timerCh := time.After(10 * time.Second)

	select {
	case <-timerCh:
		break
	case err := <-broadcastErr:
		cancelFunc()
		return errors.Wrap(err, "error running broadcast consumer")
	case err := <-directErr:
		cancelFunc()
		return errors.Wrap(err, "error running direct consumer")
	}

	go e.runServiceShutdownListener(serviceCtx)

	e.started = true

	return nil
}

func (e *Etcd) runServiceShutdownListener(serviceCtx context.Context) {
MAIN:
	for {
		select {
		case <-serviceCtx.Done():
			e.log.Debug("embedded etcd caught service exit request")
			break MAIN
		}
	}

	if err := e.Shutdown(true); err != nil {
		e.log.Errorf("unable to shutdown etcd: %s", err)
	}
}

func (e *Etcd) createClient(host string) (*clientv3.Client, error) {
	// expect dial time-out on ipv4 blackhole
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{host},
		DialTimeout: 2 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (e *Etcd) Broadcast(ctx context.Context, msg *Message) error {
	path := BroadcastPath + "/" + uuid.NewV4().String()

	return e.writeMessage(ctx, path, msg)
}

func (e *Etcd) Direct(ctx context.Context, node string, msg *Message) error {
	path := QueuePath + "/" + node + "/" + uuid.NewV4().String()

	return e.writeMessage(ctx, path, msg)
}

func (e *Etcd) writeMessage(ctx context.Context, path string, msg *Message) error {
	if !e.started {
		return ServerNotStartedErr
	}

	if path == "" {
		return errors.New("path cannot be empty")
	}

	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	msgData, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "unable to marshal msg to JSON")
	}

	if _, err := e.client.Put(ctx, path, string(msgData)); err != nil {
		return fmt.Errorf("unable to put key '%s': %s", path, err)
	}

	return nil
}

func (e *Etcd) Shutdown(force bool) error {
	if !e.started {
		return ServerNotStartedErr
	}

	e.consumerCancelFunc() // Should cause goroutines to exit

	// Give some time for consumers to exit
	time.Sleep(5 * time.Second)

	if len(e.server.Clients) != 0 && !force {
		return errors.New("active clients connected to etcd - shutdown clients first (or use force)")
	}

	e.server.Close()

	return nil
}

func (e *Etcd) launchEmbeddedEtcd(_ context.Context) (*embed.Etcd, error) {
	cfg := embed.NewConfig()

	cfg.Name = e.serverOptions.NodeId
	cfg.Dir = e.serverOptions.NodeId + ".etcd"
	cfg.LPUrls = []url.URL{*e.urls.ListenerPeerURL}
	cfg.LCUrls = []url.URL{*e.urls.ListenerClientURL}
	cfg.APUrls = []url.URL{*e.urls.AdvertisePeerURL}
	cfg.ACUrls = []url.URL{*e.urls.AdvertiseClientURL}
	cfg.InitialCluster = e.serverOptions.InitialCluster
	cfg.LogOutputs = []string{fmt.Sprintf("./%s.etcd.log", e.serverOptions.NodeId)}

	embeddedEtcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start etcd")
	}

	select {
	case <-embeddedEtcd.Server.ReadyNotify():
		e.log.Debugf("embedded etcd server '%s' has started", e.serverOptions.NodeId)
	case <-time.After(time.Minute):
		embeddedEtcd.Server.Stop()
		return nil, errors.New("etcd server took too long to start")
	}

	return embeddedEtcd, nil
}

func (e *Etcd) runDirectConsumer(serviceCtx, consumerCtx context.Context) error {
	return e.watch(serviceCtx, consumerCtx, QueuePath, e.handleDirectWatchResponse)
}

func (e *Etcd) runBroadcastConsumer(serviceCtx, consumerCtx context.Context) error {
	return e.watch(serviceCtx, consumerCtx, BroadcastPath, e.handleBroadcastWatchResponse)
}

func (e *Etcd) watch(serviceCtx, consumerCtx context.Context, path string, handlerFunc HandlerFunc) error {
	if e.client == nil {
		return errors.New("client cannot be nil")
	}

	watchChan := e.client.Watch(serviceCtx, path, clientv3.WithPrefix())

MAIN:
	for {
		select {
		case <-consumerCtx.Done():
			e.log.Debug("embedded etcd asked to exit via consumer context")
			break MAIN
		case <-serviceCtx.Done():
			e.log.Debug("embedded etcd asked to exit via service context")
			break MAIN
		case resp := <-watchChan:
			// TODO: How will watch respond to etcd server going away?

			go func() {
				if err := handlerFunc(consumerCtx, &resp); err != nil {
					e.log.Errorf("unable to handle etcd response for path '%s': %s", path, err)
				}
			}()
		}
	}

	return nil
}

func (e *Etcd) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, key, opts...)
}

func (e *Etcd) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return e.client.Put(ctx, key, val, opts...)
}

func (e *Etcd) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return e.client.Delete(ctx, key, opts...)
}

// PopulateCache loads config from etcd
func (e *Etcd) PopulateCache() error {
	if err := e.populateServerConfigCache(); err != nil {
		return errors.Wrap(err, "unable to populate server configs from cache")
	}

	if err := e.populateConnectionCache(); err != nil {
		return errors.Wrap(err, "unable to populate connection configs from cache")
	}

	if err := e.populateServiceCache(); err != nil {
		return errors.Wrap(err, "unable to populate service configs from cache")
	}

	if err := e.populateSchemaCache(); err != nil {
		return errors.Wrap(err, "unable to populate schema configs from cache")
	}

	if err := e.populateRelayCache(); err != nil {
		return errors.Wrap(err, "unable to populate relay configs from cache")
	}

	if err := e.populateValidationCache(); err != nil {
		return errors.Wrap(err, "unable to populate schema validation configs from cache")
	}

	return nil
}

func (e *Etcd) populateServerConfigCache() error {
	resp, err := e.Get(context.Background(), CacheServerConfigKey)
	if err != nil {
		return errors.Wrap(err, "unable to fetch server config from etcd")
	}

	// Nothing stored yet, do nothing
	if len(resp.Kvs) == 0 {
		return nil
	}

	cfg := &config.Config{}
	if err := json.Unmarshal(resp.Kvs[0].Value, cfg); err != nil {
		return errors.Wrap(err, "unable to unmarshal cached server config")
	}

	// These config values on the ones saved in etcd
	e.PlumberConfig.VCServiceToken = cfg.VCServiceToken
	e.PlumberConfig.GitHubToken = cfg.GitHubToken
	e.PlumberConfig.GitHubInstallID = cfg.GitHubInstallID

	// These values are the ones saved in config.json

	// TODO: Can we have some migration path from config.json to etcd? Regular plumber mode doesn't launch
	// TODO: embedded etcd, so that needs to be handled somehow

	// Wrapped in if block in case config is still stored in config.json
	if e.PlumberConfig.UserID == "" {
		e.PlumberConfig.UserID = cfg.UserID
	}

	// Wrapped in if block in case config is still stored in config.json
	if e.PlumberConfig.PlumberID == "" {
		e.PlumberConfig.PlumberID = cfg.PlumberID
	}

	// Wrapped in if block in case config is still stored in config.json
	if e.PlumberConfig.TeamID == "" {
		e.PlumberConfig.TeamID = cfg.TeamID
	}

	// Wrapped in if block in case config is still stored in config.json
	if e.PlumberConfig.Token == "" {
		e.PlumberConfig.Token = cfg.Token
	}

	return nil
}

func (e *Etcd) populateConnectionCache() error {
	resp, err := e.Get(context.Background(), CacheConnectionsPrefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "unable to fetch protos.Connection messages from etcd")
	}

	var count int

	for _, v := range resp.Kvs {
		conn := &opts.ConnectionOptions{}
		if err := proto.Unmarshal(v.Value, conn); err != nil {
			e.log.Errorf("unable to unmarshal protos.Connection message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetConnection(conn.XId, conn)
	}

	e.log.Debugf("Loaded '%d' connections from etcd", count)

	return nil
}

func (e *Etcd) populateSchemaCache() error {
	resp, err := e.Get(context.Background(), CacheSchemasPrefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "unable to fetch protos.Schema messages from etcd")
	}

	var count int

	for _, v := range resp.Kvs {
		schema := &protos.Schema{}
		if err := proto.Unmarshal(v.Value, schema); err != nil {
			e.log.Errorf("unable to unmarshal protos.Schema message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetSchema(schema.Id, schema)
	}

	e.log.Debugf("Loaded '%d' schemas from etcd", count)

	return nil
}

func (e *Etcd) populateRelayCache() error {
	resp, err := e.Get(context.Background(), CacheRelaysPrefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "unable to fetch protos.Relay messages from etcd")
	}

	var count int

	for _, v := range resp.Kvs {
		relay := &opts.RelayOptions{}
		if err := proto.Unmarshal(v.Value, relay); err != nil {
			e.log.Errorf("unable to unmarshal opts.RelayOptions message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetRelay(relay.XRelayId, &types.Relay{
			Active:  false,
			Id:      relay.XRelayId,
			Options: relay,
		})
	}

	e.log.Debugf("Loaded '%d' relays from etcd", count)

	return nil
}

func (e *Etcd) populateServiceCache() error {
	resp, err := e.Get(context.Background(), CacheServicesPrefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "unable to fetch protos.Service messages from etcd")
	}

	var count int

	for _, v := range resp.Kvs {
		svc := &protos.Service{}
		if err := proto.Unmarshal(v.Value, svc); err != nil {
			e.log.Errorf("unable to unmarshal protos.Service message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetService(svc.Id, svc)
	}

	e.log.Debugf("Loaded '%d' services from etcd", count)

	return nil
}

func (e *Etcd) populateValidationCache() error {
	resp, err := e.Get(context.Background(), CacheValidationsPrefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "unable to fetch protos.Service messages from etcd")
	}

	var count int

	for _, v := range resp.Kvs {
		validation := &common.Validation{}
		if err := proto.Unmarshal(v.Value, validation); err != nil {
			e.log.Errorf("unable to unmarshal protos.Validation message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetValidation(validation.XId, validation)
	}

	e.log.Debugf("Loaded '%d' schema validations from etcd", count)

	return nil
}

// SaveConfig marshals a config.Config to JSON and saves it to etcd so that it can be retrieved on startup
func (e *Etcd) SaveConfig(ctx context.Context, cfg *config.Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrap(err, "unable to marshal config to JSON")
	}

	_, err = e.Put(ctx, CacheServerConfigKey, string(data))
	if err != nil {
		return errors.Wrap(err, "unable to save server config to etcd")
	}

	return nil
}

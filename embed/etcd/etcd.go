package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/batchcorp/plumber-schemas/build/go/protos"

	"github.com/batchcorp/plumber/cli"
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
	Shutdown(force bool) error
	Start(serviceCtx context.Context) error

	// Message Publish helpers

	PublishCreateService(ctx context.Context, svc *protos.Service) error
	PublishUpdateService(ctx context.Context, svc *protos.Service) error
	PublishDeleteService(ctx context.Context, svc *protos.Service) error
	PublishCreateConnection(ctx context.Context, conn *protos.Connection) error
	PublishUpdateConnection(ctx context.Context, conn *protos.Connection) error
	PublishDeleteConnection(ctx context.Context, conn *protos.Connection) error
	PublishCreateSchema(ctx context.Context, schema *protos.Schema) error
	PublishUpdateSchema(ctx context.Context, schema *protos.Schema) error
	PublishDeleteSchema(ctx context.Context, schema *protos.Schema) error
	PublishCreateRelay(ctx context.Context, relay *protos.Relay) error
	PublishUpdateRelay(ctx context.Context, relay *protos.Relay) error
	PublishDeleteRelay(ctx context.Context, relay *protos.Relay) error
}

type Etcd struct {
	server             *embed.Etcd
	client             *clientv3.Client
	PlumberConfig      *config.Config
	cfg                *cli.ServerOptions
	started            bool
	consumerContext    context.Context
	consumerCancelFunc context.CancelFunc
	log                *logrus.Entry
}

var (
	ServerNotStartedErr     = errors.New("server not started")
	ServerAlreadyStartedErr = errors.New("server already started")
)

func New(cfg *cli.ServerOptions, plumberConfig *config.Config) (*Etcd, error) {
	if err := validateOptions(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &Etcd{
		cfg:           cfg,
		PlumberConfig: plumberConfig,
		log:           logrus.WithField("pkg", "etcd"),
	}, nil
}

func validateOptions(cfg *cli.ServerOptions) error {
	if cfg == nil {
		return errors.New("server options config cannot be nil")
	}

	if cfg.InitialCluster == "" {
		return errors.New("InitialCluster setting cannot be empty")
	}

	if cfg.AdvertisePeerURL == nil {
		return errors.New("AdvertisePeerURL cannot be nil")
	}

	if cfg.AdvertiseClientURL == nil {
		return errors.New("AdvertiseClientURL cannot be nil")
	}

	if cfg.ListenerPeerURL == nil {
		return errors.New("ListenerPeerURL cannot be nil")
	}

	if cfg.ListenerClientURL == nil {
		return errors.New("ListenerClientURL cannot be nil")
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

func (e *Etcd) launchEmbeddedEtcd(ctx context.Context) (*embed.Etcd, error) {
	cfg := embed.NewConfig()

	cfg.Name = e.cfg.NodeID
	cfg.Dir = e.cfg.NodeID + ".etcd"
	cfg.LPUrls = []url.URL{*e.cfg.ListenerPeerURL}
	cfg.LCUrls = []url.URL{*e.cfg.ListenerClientURL}
	cfg.APUrls = []url.URL{*e.cfg.AdvertisePeerURL}
	cfg.ACUrls = []url.URL{*e.cfg.AdvertiseClientURL}
	cfg.InitialCluster = e.cfg.InitialCluster
	cfg.LogOutputs = []string{fmt.Sprintf("./%s.etcd.log", e.cfg.NodeID)}

	embeddedEtcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start etcd")
	}

	select {
	case <-embeddedEtcd.Server.ReadyNotify():
		e.log.Debugf("embedded etcd server '%s' has started", e.cfg.NodeID)
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
	if err := e.populateConnectionCache(); err != nil {
		return err
	}

	if err := e.populateServiceCache(); err != nil {
		return err
	}

	if err := e.populateSchemaCache(); err != nil {
		return err
	}

	if err := e.populateRelayCache(); err != nil {
		return err
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
		conn := &protos.Connection{}
		if err := proto.Unmarshal(v.Value, conn); err != nil {
			e.log.Errorf("unable to unmarshal protos.Connection message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetConnection(conn.Id, conn)
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
		relay := &protos.Relay{}
		if err := proto.Unmarshal(v.Value, relay); err != nil {
			e.log.Errorf("unable to unmarshal protos.Relay message: %s", err)
			continue
		}

		count++

		e.PlumberConfig.SetRelay(relay.RelayId, &types.Relay{
			Active: false,
			Id:     relay.RelayId,
			Config: relay,
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

package etcd

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/batchcorp/plumber/cli"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	BroadcastPath = "/bus/broadcast"
	QueuePath     = "/bus/queue" // actually /bus/queue/$nodeID/<msg>
)

type Etcd struct {
	server  *embed.Etcd
	cfg     *cli.ServerOptions
	started bool
	log     *logrus.Entry
}

var (
	ServerNotStartedErr     = errors.New("server not started")
	ServerAlreadyStartedErr = errors.New("server already started")
)

func New(cfg *cli.ServerOptions) (*Etcd, error) {
	if err := validateOptions(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &Etcd{
		cfg: cfg,
		log: logrus.WithField("pkg", "etcd"),
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

func (e *Etcd) Start(ctx context.Context) error {
	if e.started {
		return ServerAlreadyStartedErr
	}

	broadcastErr := make(chan error, 1)
	directErr := make(chan error, 1)

	embeddedEtcd, err := e.launchEmbeddedEtcd(ctx)
	if err != nil {
		e.log.Warningf("embedded etcd exit due to err: %s", err)
		return errors.Wrap(err, "unable to launch embedded etcd")
	}

	// Start broadcast consumer
	go func() {
		if err := e.runBroadcastConsumer(ctx); err != nil {
			e.log.Warningf("etcd broadcast consumer exit due to err: %s", err)

			broadcastErr <- err
		}
	}()

	// Start direct consumer
	go func() {
		if err := e.runDirectConsumer(ctx); err != nil {
			e.log.Warningf("etcd direct consuemr exit due to err: %s", err)
			directErr <- err
		}
	}()

	// Listen for errors (for a minute)
	timerCh := time.After(time.Minute)

	select {
	case <-timerCh:
		break
	case err := <-broadcastErr:
		return errors.Wrap(err, "error running broadcast consumer")
	case err := <-directErr:
		return errors.Wrap(err, "error running direct consumer")
	}

	e.server = embeddedEtcd
	e.started = true

	return nil
}

// TODO: Implement (etcd)
func (e *Etcd) Broadcast(ctx context.Context, msg *Message) error {
	if !e.started {
		return ServerNotStartedErr
	}

	return nil
}

// TODO: Implement (etcd)
func (e *Etcd) Direct(ctx context.Context, node string, msg *Message) error {
	if !e.started {
		return ServerNotStartedErr
	}

	return nil
}

// TODO: Implement (etcd)
func (e *Etcd) Shutdown() error {
	if !e.started {
		return ServerNotStartedErr
	}

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

package monitor

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	LeaderWatcherInterval = 5 * time.Second
)

type IMonitor interface {
	RunElectLeader(ctx context.Context, ch chan *LeaderStatus, path string)
}

type Monitor struct {
	etcdClient          *clientv3.Client
	electLeaderLooper   director.Looper
	leaderWatcherLooper director.Looper
	runLeaderLooper     director.Looper
	quitElectLeader     bool
	nodeID              string
	log                 *logrus.Entry
}

func New(etcdClient *clientv3.Client, nodeID string) (*Monitor, error) {
	if err := validateOpts(etcdClient, nodeID); err != nil {
		return nil, errors.Wrap(err, "unable to validate opts")
	}

	return &Monitor{
		etcdClient:          etcdClient,
		electLeaderLooper:   director.NewFreeLooper(director.FOREVER, nil),
		leaderWatcherLooper: director.NewTimedLooper(director.FOREVER, LeaderWatcherInterval, nil),
		runLeaderLooper:     director.NewFreeLooper(director.FOREVER, nil),
		nodeID:              nodeID,
		log: logrus.WithFields(logrus.Fields{
			"pkg":  "monitor",
			"node": nodeID,
		}),
	}, nil
}

func validateOpts(client *clientv3.Client, nodeID string) error {
	if client == nil {
		return errors.New("etcd client cannot be nil")
	}

	if nodeID == "" {
		return errors.New("nodeID cannot be empty")
	}

	return nil
}

package monitor

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	RetryCampaignInterval        = 5 * time.Second
	DefaultEtcdSessionTTLSeconds = 5
)

type LeaderStatus struct {
	NodeID string
}

// RunElectLeader is the main entrypoint for launching a monitoring instance.
// It will elect and launch a leader + continue monitoring for leadership changes.
//
// Flow:
//
// 1. In a loop, Campaign() to become leader
// 2. Campaign() blocks until leader is acquired
// 3. When leader is acquired:
//  - Launch runLeader
//	- Launch runLeaderWatcher
// 4. runLeaderWatcher verifies that we are still leader AND monitors shutdown
//    ctx. If either has changed/triggered: it will shutdown RunLeader and itself.
// 5. runLeader has watches for incoming alerts/etc.
//
// Etcd layout
//
// /$cluster-id/monitor/leader
// /$cluster-id/monitor/config/dedicated-*
// /$cluster-id/monitor/config/shared-*
// /$cluster-id/monitor/metrics/$check-id
func (m *Monitor) RunElectLeader(ctx context.Context, ch chan *LeaderStatus, path string) error {
	llog := m.log.WithField("method", "RunLeaderElect")

	llog.Debug("started leader election")

	// TODO: Should we enforce that this is operated as a singleton?

	var election *concurrency.Election

	m.electLeaderLooper.Loop(func() error {
		// Give director a little to catch up
		if m.quitElectLeader {
			llog.Debug("in the process of quitting - nothing to do")
			time.Sleep(5 * time.Second)
			return nil
		}

		// Any existing leader?
		leaderInfo, err := m.Leader(ctx, election)
		if err != nil {
			llog.Errorf("unable to get existing leader info: %s (retrying in %s)", err, RetryCampaignInterval)
			time.Sleep(RetryCampaignInterval)
			return nil
		}

		if leaderInfo.NodeID == m.nodeID {
			llog.Debug("still leader, no need to campaign")
			time.Sleep(RetryCampaignInterval)
			return nil
		}

		session, err := concurrency.NewSession(m.etcdClient, concurrency.WithTTL(DefaultEtcdSessionTTLSeconds))
		if err != nil {
			// TODO: This needs to be beefier
			llog.Errorf("unable to create new etcd session: %s", err)
			time.Sleep(5 * time.Second)
			return nil
		}

		election = concurrency.NewElection(session, path)

		// Once leader is elected, it will unblock; everyone else continues to
		// block.
		if err := election.Campaign(ctx, m.nodeID); err != nil {
			if err == context.Canceled {
				llog.Warning("context cancelled - quitting")
				m.quitElectLeader = true
				m.electLeaderLooper.Quit()

				return nil
			}

			llog.Errorf("unable to complete campaign: %s (retrying in %s)", err, RetryCampaignInterval)
			time.Sleep(RetryCampaignInterval)
			return nil
		}

		llog.Debug("leader election succeeded")

		go m.runLeader(ctx)
		go m.runLeaderWatcher(ctx, election)

		return nil
	})

	llog.Debug("exiting")

	return nil
}

// Leader fetches the value that is set by the leader.
func (m *Monitor) Leader(ctx context.Context, election *concurrency.Election) (*LeaderStatus, error) {
	resp, err := election.Leader(ctx)
	if err != nil {
		if err == concurrency.ErrElectionNoLeader {
			return &LeaderStatus{
				NodeID: "",
			}, nil
		}

		return nil, errors.Wrap(err, "error fetching leader value")
	}

	// Should have at least one KV - anything less is an unexpected error
	if len(resp.Kvs) < 1 {
		return nil, errors.New("unexpected number of kvs in etcd resp")
	}

	// Leader exists
	return &LeaderStatus{
		NodeID: string(resp.Kvs[0].Value),
	}, nil
}

func (m *Monitor) runLeader(ctx context.Context) {
	llog := m.log.WithField("method", "runLeader")

	llog.Debug("starting")

	m.runLeaderLooper.Loop(func() error {
		// Receive outbound alerts
		// Perform leader monitoring duties

		return nil
	})

	llog.Debug("exiting")
}

// In a loop, watch the ctx done chan and verify that we are still leader and
// someone hasn't taken over instead. If a new leader has been elected while
// we *think* we are leader - shutdown runLeader and runLeaderWatcher.
func (m *Monitor) runLeaderWatcher(ctx context.Context, election *concurrency.Election) {
	llog := m.log.WithField("method", "runLeaderWatcher")

	llog.Debug("starting")

	errRetry := 5 * time.Second
	var quit bool

	m.leaderWatcherLooper.Loop(func() error {
		if quit {
			// Give looper time to pick up the quit
			llog.Debug("quitting - waiting for looper to catch up, nothing to do")

			time.Sleep(5 * time.Second)
			return nil
		}

		// Are we still leader?
		leaderStatus, err := m.Leader(ctx, election)
		if err != nil {
			llog.Errorf("unable to determine leader info: %s (retrying in %s)", err, errRetry)
			time.Sleep(errRetry)

			return nil
		}

		if leaderStatus.NodeID != m.nodeID {
			llog.Debug("no longer leader - shutting down runLeader and myself")
			m.cancelRunLeader()
			m.leaderWatcherLooper.Quit()
			quit = true

			return nil
		}

		// Non-blocking read ctx done chan
		select {
		case <-ctx.Done():
			llog.Debug("leader context cancelled - shutting down runLeader and myself")
			m.cancelRunLeader()
			m.leaderWatcherLooper.Quit()
			quit = true
		default:
			// NOOP
		}

		return nil
	})

	llog.Debug("exiting")
}

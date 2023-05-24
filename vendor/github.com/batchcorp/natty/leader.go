package natty

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
)

const (
	DefaultAsLeaderBucketTTL              = time.Second * 10
	DefaultAsLeaderElectionLooperInterval = time.Second
	DefaultAsLeaderReplicaCount           = 1
)

var (
	ErrBucketTTLMismatch = errors.New("bucket ttl mismatch")
)

type AsLeaderConfig struct {
	// Looper is the loop construct that will be used to execute Func (required)
	Looper director.Looper

	// Bucket specifies what K/V bucket will be used for leader election (required)
	Bucket string

	// Key specifies the keyname that the leader election will occur on (required)
	Key string

	// NodeName is the name used for this node; should be unique in cluster (required)
	NodeName string

	// Description will set the bucket description (optional)
	Description string

	// ElectionLooper allows you to override the used election looper (optional)
	ElectionLooper director.Looper

	// BucketTTL specifies the TTL policy the bucket should use (optional)
	BucketTTL time.Duration

	// ReplicaCount specifies the number of replicas the bucket should use (optional, default 1)
	ReplicaCount int
}

func (n *Natty) GetLeader(ctx context.Context, bucketName, keyName string) (string, error) {
	currentLeader, err := n.Get(ctx, bucketName, keyName)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return "", err
		}

		n.log.Errorf("unable to determine leader for '%s:%s': %s", bucketName, keyName, err)

		return "", fmt.Errorf("unable to determine leader for '%s:%s': %s", bucketName, keyName, err)
	}

	return string(currentLeader), nil
}

func (n *Natty) HaveLeader(ctx context.Context, nodeName, bucketName, keyName string) bool {
	currentLeader, err := n.GetLeader(ctx, bucketName, keyName)
	if err != nil {
		return false
	}

	return currentLeader == nodeName
}

func (n *Natty) AsLeader(ctx context.Context, cfg AsLeaderConfig, f func() error) error {
	if err := validateAsLeaderArgs(&cfg, f); err != nil {
		return errors.Wrap(err, "unable to validate AsLeader args")
	}

	// Attempt to create bucket; if bucket exists, verify that TTL matches
	if err := n.CreateBucket(ctx, cfg.Bucket, cfg.BucketTTL, cfg.ReplicaCount, cfg.Description); err != nil {
		if strings.Contains(err.Error(), "stream name already in use") {
			n.log.Debug("bucket exists, checking if ttl matches")

			kv, err := n.js.KeyValue(cfg.Bucket)
			if err != nil {
				return errors.Wrap(err, "unable to fetch existing bucket")
			}

			s, err := kv.Status()
			if err != nil {
				return errors.Wrap(err, "unable to fetch existing bucket status")
			}

			n.log.Debugf("ttl on bucket: %v desired ttl: %v", s.TTL(), cfg.BucketTTL)

			if s.TTL().Seconds() != cfg.BucketTTL.Seconds() {
				n.log.Error("bucket ttls do not match")
				return ErrBucketTTLMismatch
			}
		} else {
			return errors.Wrap(err, "unable to pre-create leader bucket")
		}
	}

	errCh := make(chan error, 1)

	n.log.Debugf("%s: starting leader election goroutine", cfg.NodeName)

	// Launch leader election in goroutine (leader election goroutine should quit when AsLeader is cancelled or exits)
	go func() {
		err := n.runLeaderElection(ctx, &cfg)
		if err != nil {
			n.log.Errorf("%s: unable to run leader election: %v", cfg.NodeName, err)
			errCh <- err

			return
		}

		n.log.Debugf("%s: leader election goroutine exited", cfg.NodeName)
	}()

	n.log.Debugf("%s: waiting for goroutine to not error", cfg.NodeName)

	select {
	case err := <-errCh:
		return errors.Wrap(err, "ran into error during leader election startup")
	case <-ctx.Done():
		return errors.New("context cancelled - leader election cancelled")
		// Leader election did not error after 2 seconds, all is well
	case <-time.After(time.Second * 1):
		break
	}

	// Leader election goroutine started; run main loop
	n.log.Debugf("%s: leader election goroutine started; running main loop", cfg.NodeName)

	var quit bool

	cfg.Looper.Loop(func() error {
		if quit {
			// Give looper time to catch up
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		select {
		case <-ctx.Done():
			n.log.Debugf("%s: AsLeader cancelled - exiting loop func", cfg.NodeName)

			quit = true
			cfg.Looper.Quit()

			return nil
		default:
			// Continue
		}

		if !n.HaveLeader(ctx, cfg.NodeName, cfg.Bucket, cfg.Key) {
			n.log.Debugf("%s: AsLeader: not leader", cfg.NodeName)
			return nil
		}

		n.log.Debugf("%s: AsLeader: running func", cfg.NodeName)

		// Have leader, exec func
		if err := f(); err != nil {
			n.log.Errorf("%s: error during func execution: %v", cfg.NodeName, err)
			return nil
		}

		return nil
	})

	n.log.Debugf("%s: AsLeader func loop exited for '%s'", cfg.NodeName, cfg.Bucket, cfg.Key)

	return nil
}

func (n *Natty) runLeaderElection(ctx context.Context, cfg *AsLeaderConfig) error {
	var quit bool

	var haveLeader bool

	cfg.ElectionLooper.Loop(func() error {
		// We are supposed to quit - give looper time to react to quit
		if quit {
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		// NATS K/V client does not support ctx yet so we do it here instead
		select {
		case <-ctx.Done():
			n.log.Debugf("%s: context cancelled, exiting leader election", cfg.NodeName)
			quit = true
			cfg.ElectionLooper.Quit()

			return nil
		default:
			// Continue
		}

		// Have leader - attempt to update key to increase TTL
		if n.HaveLeader(ctx, cfg.NodeName, cfg.Bucket, cfg.Key) {
			if err := n.Put(ctx, cfg.Bucket, cfg.Key, []byte(cfg.NodeName), cfg.BucketTTL); err != nil {
				// Something happened, try again next iteration - maybe we're
				// still the leader.
				n.log.Errorf("%s: unable to update leader key '%s:%s': %v", cfg.NodeName, cfg.Bucket, cfg.Key, err)

				return nil
			}

			haveLeader = true

			n.log.Debugf("%s: updated leader key '%s:%s'", cfg.NodeName, cfg.Bucket, cfg.Key)

			return nil
		} else {
			if haveLeader {
				n.log.Infof("%s: lost leader '%s:%s'", cfg.NodeName, cfg.Bucket, cfg.Key)
			}

			haveLeader = false
		}

		if err := n.Create(ctx, cfg.Bucket, cfg.Key, []byte(cfg.NodeName), cfg.BucketTTL); err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				n.log.Debugf("%s: leader key already exists, ignoring", cfg.NodeName)
				return nil
			}

			n.log.Errorf("%s: unable to create leader key '%s:%s': %v", cfg.NodeName, cfg.Bucket, cfg.Key, err)

			return nil
		}

		n.log.Infof("%s: became leader '%s:%s'", cfg.NodeName, cfg.Bucket, cfg.Key)

		haveLeader = true

		return nil
	})

	n.log.Debugf("%s: leader election goroutine exiting", cfg.NodeName)

	return nil
}

func validateAsLeaderArgs(cfg *AsLeaderConfig, f func() error) error {
	if cfg == nil {
		return errors.New("AsLeaderConfig is required")
	}

	if cfg.Looper == nil {
		return errors.New("Looper is required")
	}

	if cfg.Bucket == "" {
		return errors.New("Bucket is required")
	}

	if cfg.Key == "" {
		return errors.New("Key is required")
	}

	if cfg.NodeName == "" {
		return errors.New("NodeName is required")
	}

	if cfg.ReplicaCount == 0 {
		cfg.ReplicaCount = DefaultAsLeaderReplicaCount
	}

	if f == nil {
		return errors.New("Function cannot be nil")
	}

	if cfg.ElectionLooper == nil {
		cfg.ElectionLooper = director.NewTimedLooper(director.FOREVER, DefaultAsLeaderElectionLooperInterval, make(chan error, 1))
	}

	if cfg.BucketTTL == 0 {
		cfg.BucketTTL = DefaultAsLeaderBucketTTL
	}

	return nil
}

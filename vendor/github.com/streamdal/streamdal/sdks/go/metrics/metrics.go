// Package metrics is responsible for tracking and publishing metrics to the Streamdal server.
package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"

	"github.com/streamdal/streamdal/sdks/go/logger"
	"github.com/streamdal/streamdal/sdks/go/server"
	"github.com/streamdal/streamdal/sdks/go/types"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IMetrics
type IMetrics interface {
	// Incr increases a counter with the given entry.
	// If a counter does not exist for this entry yet, one will be created.
	Incr(ctx context.Context, entry *types.CounterEntry) error
}

const (
	// defaultIncrInterval defines how often we process the increase queue
	defaultIncrInterval = time.Second

	// defaultReaperInterval is how often the reaper goroutine will delete stale counters
	// A stale counter is one with zero value and last updated time > ReaperTTL
	defaultReaperInterval = 10 * time.Second

	// defaultReaperTTL is how long a counter can be stale before it is reaped
	// A stale counter is one with zero value and last updated time > ReaperTTL
	defaultReaperTTL = 10 * time.Second

	// defaultWorkerPoolSize is how many counter workers will be spun up.
	// These workers are responsible for processing the counterIncrCh and counterPublishCh channels
	defaultWorkerPoolSize = 1

	// serverFlushTimeout is the maximum amount of time to wait before flushing metrics to the server
	serverFlushTimeout = time.Second * 2

	// serverFlushMaxBatchSize is the largest a batch of metrics will grow before being flushed to the server
	serverFlushMaxBatchSize = 100
)

var (
	ErrMissingConfig       = errors.New("config cannot be nil")
	ErrMissingServerClient = errors.New("ServerClient cannot be nil")
	ErrMissingEntry        = errors.New("CounterEntry cannot be nil")
	ErrEmptyName           = errors.New("Name must be set")
	ErrMissingShutdownCtx  = errors.New("ShutdownCtx cannot be nil")
)

type Metrics struct {
	*Config

	wg                  *sync.WaitGroup
	counterMapMutex     *sync.RWMutex
	counterTickerLooper director.Looper
	counterReaperLooper director.Looper
	counterMap          map[string]*counter
	counterIncrCh       chan *types.CounterEntry
	counterPublishCh    chan *types.CounterEntry
}

type Config struct {
	IncrInterval   time.Duration
	ReaperInterval time.Duration
	ReaperTTL      time.Duration
	WorkerPoolSize int
	ServerClient   server.IServerClient
	ShutdownCtx    context.Context
	Log            logger.Logger
}

func New(cfg *Config) (*Metrics, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	m := &Metrics{
		Config:              cfg,
		counterMap:          make(map[string]*counter),
		counterMapMutex:     &sync.RWMutex{},
		counterTickerLooper: director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		counterReaperLooper: director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		counterIncrCh:       make(chan *types.CounterEntry, 10000),
		counterPublishCh:    make(chan *types.CounterEntry, 10000),
		wg:                  &sync.WaitGroup{},
	}

	// Launch counter worker pool
	for i := 0; i < m.Config.WorkerPoolSize; i++ {
		m.wg.Add(1)
		go m.runCounterWorkerPool(
			fmt.Sprintf("worker-%d", i),
			director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		)
	}

	// Launch counter ticker
	m.wg.Add(1)
	go m.runCounterTicker()

	// Launch counter reaper
	m.wg.Add(1)
	go m.runCounterReaper()

	return m, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return ErrMissingConfig
	}

	if cfg.ServerClient == nil {
		return ErrMissingServerClient
	}

	if cfg.ShutdownCtx == nil {
		return ErrMissingShutdownCtx
	}

	applyDefaults(cfg)

	return nil
}

func applyDefaults(cfg *Config) {
	if cfg.IncrInterval == 0 {
		cfg.IncrInterval = defaultIncrInterval
	}

	if cfg.ReaperInterval == 0 {
		cfg.ReaperInterval = defaultReaperInterval
	}

	if cfg.ReaperTTL == 0 {
		cfg.ReaperTTL = defaultReaperTTL
	}

	// Cannot have a worker pool size of 0
	if cfg.WorkerPoolSize == 0 {
		cfg.WorkerPoolSize = defaultWorkerPoolSize
	}

	if cfg.Log == nil {
		cfg.Log = &logger.TinyLogger{}
	}
}

func (m *Metrics) Incr(_ context.Context, entry *types.CounterEntry) error {
	if err := validateCounterEntry(entry); err != nil {
		return errors.Wrap(err, "unable to validate counter entry")
	}

	// counterIncrCh is a buffered channel (10k by default) so Incr() shouldn't
	// generally block. If it does, it is because the worker pool is lagging behind.
	m.counterIncrCh <- entry

	return nil
}

func validateCounterEntry(entry *types.CounterEntry) error {
	if entry == nil {
		return ErrMissingEntry
	}

	if entry.Name == "" {
		return ErrEmptyName
	}

	return nil
}

func (m *Metrics) newCounter(e *types.CounterEntry) *counter {
	m.counterMapMutex.Lock()
	defer m.counterMapMutex.Unlock()

	c := &counter{
		entry:      e,
		countMutex: &sync.RWMutex{},
	}

	m.counterMap[compositeID(e)] = c

	return c
}

func (m *Metrics) getCounter(e *types.CounterEntry) (*counter, bool) {
	m.counterMapMutex.RLock()
	defer m.counterMapMutex.RUnlock()

	if counter, ok := m.counterMap[compositeID(e)]; ok {
		return counter, true
	}

	return nil, false
}

// getCounters fetches active counters
func (m *Metrics) getCounters() map[string]*counter {
	m.counterMapMutex.RLock()
	defer m.counterMapMutex.RUnlock()

	localCounters := make(map[string]*counter)

	for counterID, counter := range m.counterMap {
		localCounters[counterID] = counter
	}

	return localCounters
}

func (m *Metrics) incr(_ context.Context, entry *types.CounterEntry) error {
	// No need to validate - no way to reach here without validate
	c, ok := m.getCounter(entry)
	if ok {
		c.incr(entry)
		return nil
	}

	c = m.newCounter(entry)
	c.incr(entry)

	return nil
}

// runCounterWorkerPool is responsible for listening for Incr() requests and
// flush requests (from ticker runner).
func (m *Metrics) runCounterWorkerPool(_ string, looper director.Looper) {
	defer m.wg.Done()

	var (
		err      error
		shutdown bool
	)

	// Batch metrics entries to send to server
	metricsEntries := make([]*types.CounterEntry, 0)
	lastFlush := time.Now().UTC()

	looper.Loop(func() error {
		// Give looper a moment to catch up
		if shutdown {
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		select {
		case entry := <-m.counterIncrCh: // Coming from user doing Incr() // buffered chan
			err = m.incr(context.Background(), entry)
		case entry := <-m.counterPublishCh: // Coming from ticker runner
			m.Log.Debugf("received publish for counter '%s', getValue: %d", entry.Name, entry.Value)
			metricsEntries = append(metricsEntries, entry)
		case <-m.ShutdownCtx.Done():
			m.Log.Debugf("received notice to shutdown")
			looper.Quit()
			shutdown = true

			return nil
		}

		if err != nil {
			m.Log.Errorf("worker pool error: %s", err)
		}

		if len(metricsEntries) > serverFlushMaxBatchSize || time.Now().UTC().Sub(lastFlush) > serverFlushTimeout {
			err = m.ServerClient.SendMetrics(context.Background(), metricsEntries)

			// Reset regardless of errors. We don't want to eat up memory or block channels
			metricsEntries = make([]*types.CounterEntry, 0)
			lastFlush = time.Now().UTC()

			if err != nil && strings.Contains(err.Error(), "connection refused") {
				// Server went away, log, sleep, and wait for reconnect
				m.Log.Warn("failed to send metrics, streamdal server went away, waiting for reconnect")
				time.Sleep(time.Second * 5)
				return nil
			}
		}

		return nil
	})

	m.Log.Debugf("exiting runCounterWorkerPool")
}

func (m *Metrics) runCounterTicker() {
	defer m.wg.Done()

	var shutdown bool

	ticker := time.NewTicker(m.Config.IncrInterval)

	m.counterTickerLooper.Loop(func() error {
		// Give looper a moment to catch up
		if shutdown {
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		select {
		case <-ticker.C:
			counters := m.getCounters()

			for _, counter := range counters {
				counterValue := counter.getValue()

				// Do not publish zero values
				if counterValue == 0 {
					continue
				}

				// Get CounterEntry w/ overwritten value
				entry := counter.getEntry()

				// Reset counter back to 0
				counter.reset()

				// If this blocks, it indicates that worker pool is lagging behind
				m.counterPublishCh <- &entry
			}
		case <-m.ShutdownCtx.Done():
			m.Log.Debugf("received notice to shutdown")
			m.counterTickerLooper.Quit()
			shutdown = true
		}

		return nil
	})

	m.Log.Debugf("exiting runCounterTicker()")
}

func (m *Metrics) runCounterReaper() {
	defer m.wg.Done()

	var shutdown bool

	ticker := time.NewTicker(m.Config.ReaperInterval)

	m.counterReaperLooper.Loop(func() error {
		if shutdown {
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		select {
		case <-ticker.C:
			counters := m.getCounters()

			for counterID, counter := range counters {
				// Do not reap active counters
				if counter.getValue() != 0 {
					m.Log.Debugf("skipping reaping for zero counter '%s'", counter.entry)
					continue
				}

				if time.Now().Sub(counter.getLastUpdated()) < m.Config.ReaperTTL {
					m.Log.Debugf("skipping reaping for non-stale counter '%s'", counter.entry)
					continue
				}

				m.Log.Debugf("reaped stale counter '%s'", counter.entry)

				m.counterMapMutex.Lock()
				delete(m.counterMap, counterID)
				m.counterMapMutex.Unlock()
			}
		case <-m.ShutdownCtx.Done():
			m.Log.Debugf("received notice to shutdown")
			m.counterReaperLooper.Quit()
			shutdown = true

			return nil
		}

		return nil
	})

	m.Log.Debugf("exiting runCounterReaper()")
}

package bus

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/natty"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/actions"
	"github.com/batchcorp/plumber/config"
)

const (
	StreamName       = "events"
	BroadcastSubject = "broadcast"
	QueueSubject     = "queue"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IBus
type IBus interface {
	// Start starts up the broadcast and queue consumers
	Start(serviceCtx context.Context) error

	// Stop stops the broadcast and queue consumers
	Stop() error

	// Convenience methods for publishing messages on the bus (without having
	// to know if they are intended to be broadcast or queue messages)

	PublishCreateConnection(ctx context.Context, conn *opts.ConnectionOptions) error
	PublishUpdateConnection(ctx context.Context, conn *opts.ConnectionOptions) error
	PublishDeleteConnection(ctx context.Context, conn *opts.ConnectionOptions) error

	PublishCreateRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishUpdateRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishDeleteRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishStopRelay(ctx context.Context, relay *opts.RelayOptions) error
	PublishResumeRelay(ctx context.Context, relay *opts.RelayOptions) error

	PublishCreateTunnel(ctx context.Context, tunnelOptions *opts.TunnelOptions) error
	PublishUpdateTunnel(ctx context.Context, tunnelOptions *opts.TunnelOptions) error
	PublishStopTunnel(ctx context.Context, tunnelOptions *opts.TunnelOptions) error
	PublishResumeTunnel(ctx context.Context, tunnelOptions *opts.TunnelOptions) error
	PublishDeleteTunnel(ctx context.Context, tunnelOptions *opts.TunnelOptions) error
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
	Actions          actions.IActions
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

	// Run broadcast consumer
	go func() {
		if err := b.runBroadcastConsumer(consumerCtx); err != nil {
			b.log.Warningf("broadcast consumer exit due to err: %s", err)
			broadcastErrChan <- err
		}
	}()

	b.log.Debug("starting queue consumer")

	// Run queue consumer
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
			b.log.Debug("service shutdown detected, stopping consumer error watcher")
			b.consumerErrorLooper.Quit()
			quit = true
		case <-consumerCtx.Done():
			b.log.Debug("consumer shutdown detected, stopping consumer error watcher")
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
	broadcastCfg.ConsumerFilterSubject = StreamName + "." + BroadcastSubject

	broadcastClient, err := natty.New(&broadcastCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create broadcast client")
	}

	queueCfg := nattyCfg

	// By assigning a common consumer name, NATS will deliver the message to
	// only one plumber instance.
	queueCfg.ConsumerName = "queue-consumer"
	queueCfg.ConsumerFilterSubject = StreamName + "." + QueueSubject

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
	subject := StreamName + "." + QueueSubject

	llog := b.log.WithFields(logrus.Fields{
		"consumer": "broadcast",
		"subject":  subject,
	})

	for {
		err := b.queueClient.Consume(consumerCtx, subject, b.consumerErrChan, b.queueCallback)
		if err != nil {
			if err == context.Canceled {
				llog.Debug("queue consumer context cancelled")
				break
			}
		}
	}

	llog.Debug("exiting")

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

package relay

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	sqsTypes "github.com/batchcorp/plumber/backends/awssqs/types"
	postgresTypes "github.com/batchcorp/plumber/backends/cdcpostgres/types"
	kafkaTypes "github.com/batchcorp/plumber/backends/kafka/types"
	rstreamsTypes "github.com/batchcorp/plumber/backends/rstreams/types"
	"github.com/batchcorp/plumber/prometheus"
)

const (
	// DefaultNumWorkers is the number of goroutine relay workers to launch
	DefaultNumWorkers = 10

	// QueueFlushInterval is how often to flush messages to GRPC collector if we don't reach the batch size
	QueueFlushInterval = 10 * time.Second

	// DefaultBatchSize is the number of messages to send to GRPC collector in each batch
	DefaultBatchSize = 100 // number of messages to batch

	// MaxGRPCRetries is the number of times we will attempt a GRPC call before giving up
	MaxGRPCRetries = 5

	// MaxGRPCMessageSize is the maximum message size for GRPC client in bytes
	MaxGRPCMessageSize = 1024 * 1024 * 100 // 100MB

	// GRPCRetrySleep determines how long we sleep between GRPC call retries
	GRPCRetrySleep = time.Second * 5
)

var (
	ErrMissingConfig             = errors.New("Relay config cannot be nil")
	ErrMissingToken              = errors.New("Token cannot be empty")
	ErrMissingServiceShutdownCtx = errors.New("ServiceShutdownCtx cannot be nil")
	ErrMissingGRPCAddress        = errors.New("GRPCAddress cannot be empty")
	ErrMissingRelayCh            = errors.New("RelayCh cannot be nil")
)

type IRelayBackend interface {
	Relay() error
}

type Relay struct {
	*Config
	Workers      map[int32]struct{}
	WorkersMutex *sync.RWMutex
	log          *logrus.Entry
}

type Config struct {
	Token              string
	GRPCAddress        string
	NumWorkers         int32
	BatchSize          int32
	RelayCh            chan interface{}
	ErrorCh            chan *records.ErrorRecord
	DisableTLS         bool
	Timeout            time.Duration // general grpc timeout (used for all grpc calls)
	Type               string
	ServiceShutdownCtx context.Context
	MainShutdownFunc   context.CancelFunc
}

// New creates a new instance of the Relay
func New(relayCfg *Config) (*Relay, error) {
	if err := validateConfig(relayCfg); err != nil {
		return nil, errors.Wrap(err, "unable to complete relay config validation")
	}

	// Verify grpc connection & token
	if err := TestConnection(relayCfg); err != nil {
		return nil, errors.Wrap(err, "unable to complete connection test")
	}

	return &Relay{
		Config:       relayCfg,
		Workers:      make(map[int32]struct{}),
		WorkersMutex: &sync.RWMutex{},
		log:          logrus.WithField("pkg", "relay"),
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return ErrMissingConfig
	}

	if cfg.Token == "" {
		return ErrMissingToken
	}

	if cfg.GRPCAddress == "" {
		return ErrMissingGRPCAddress
	}

	if cfg.RelayCh == nil {
		return ErrMissingRelayCh
	}

	if cfg.ServiceShutdownCtx == nil {
		return ErrMissingServiceShutdownCtx
	}

	if cfg.NumWorkers <= 0 {
		logrus.Warningf("NumWorkers cannot be <= 0 - setting to default '%d'", DefaultNumWorkers)
		cfg.NumWorkers = DefaultNumWorkers
	}

	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}

	return nil
}

func TestConnection(cfg *Config) error {
	conn, ctx, err := NewConnection(cfg.GRPCAddress, cfg.Token, cfg.Timeout, cfg.DisableTLS, false)
	if err != nil {
		return errors.Wrap(err, "unable to create new connection")
	}

	// Call the Test method to verify connectivity
	c := services.NewGRPCCollectorClient(conn)

	if _, err := c.Test(ctx, &services.TestRequest{}); err != nil {
		return errors.Wrap(err, "unable to complete Test request")
	}

	return nil
}

func NewConnection(address, token string, timeout time.Duration, disableTLS, noCtx bool) (*grpc.ClientConn, context.Context, error) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}

	if !disableTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(
			&tls.Config{
				InsecureSkipVerify: true,
			},
		)))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	dialContext, _ := context.WithTimeout(context.Background(), timeout)

	conn, err := grpc.DialContext(dialContext, address, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to grpc address '%s': %s", address, err)
	}

	var ctx context.Context

	if !noCtx {
		ctx, _ = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx = context.Background()
	}

	md := metadata.Pairs("batch-token", token)
	outCtx := metadata.NewOutgoingContext(ctx, md)

	return conn, outCtx, nil
}

// WaitForShutdown will wait for service shutdown context to be canceled. It will then start constantly polling
// the workers map until it is empty. When the workers map is empty, MainShutdownFunc() is called allowing the
// application to exit gracefully and ensuring we have sent all relay messages to the grpc-collector
func (r *Relay) WaitForShutdown() {

	// Don't start looping until we are in shutdown mode
	<-r.ServiceShutdownCtx.Done()

	r.log.Debugf("Waiting for relay workers to shut down")

	for {
		r.WorkersMutex.RLock()
		if len(r.Workers) == 0 {
			r.WorkersMutex.RUnlock()
			r.log.Debug("All relay workers shutdown, exiting application")
			r.MainShutdownFunc()
			return
		}
		r.WorkersMutex.RUnlock()

		time.Sleep(time.Millisecond * 100)

	}
}

// removeWorker removes a worker from the workers map so we can track when all workers have shut down
func (r *Relay) removeWorker(id int32) {
	r.WorkersMutex.Lock()
	defer r.WorkersMutex.Unlock()
	delete(r.Workers, id)
}

// addWorker adds a worker ID to the workers map
func (r *Relay) addWorker(id int32) {
	r.WorkersMutex.Lock()
	defer r.WorkersMutex.Unlock()
	r.Workers[id] = struct{}{}
}

func (r *Relay) StartWorkers(shutdownCtx context.Context) error {
	for i := int32(0); i != r.Config.NumWorkers; i++ {
		r.log.WithField("workerId", i).Debug("starting worker")

		conn, outboundCtx, err := NewConnection(r.Config.GRPCAddress, r.Config.Token, r.Config.Timeout, r.Config.DisableTLS, true)
		if err != nil {
			return fmt.Errorf("unable to create new gRPC connection for worker %d: %s", i, err)
		}

		go r.Run(i, conn, outboundCtx, shutdownCtx)
	}

	return nil
}

// Run is a GRPC worker that runs as a goroutine. outboundCtx is used for sending GRPC requests as it will contain
// metadata, specifically "batch-token". shutdownCtx is passed from the main plumber app to shut down workers
// TODO: This should also read from errorCh
func (r *Relay) Run(id int32, conn *grpc.ClientConn, outboundCtx, shutdownCtx context.Context) {
	llog := r.log.WithField("relayId", id)
	r.addWorker(id)

	llog.Debug("Relayer started")

	queue := make([]interface{}, 0)

	// This functions as an escape-vale -- if we are pumping messages *REALLY*
	// fast - we will hit max queue size; if we are pumping messages slowly,
	// the ticker will be hit and the queue will be flushed, regardless of size.
	flushTicker := time.NewTicker(QueueFlushInterval)

	// These are only here to provide immediate feedback that stats are enabled
	prometheus.Incr(r.Config.Type+"-relay-consumer", 0)
	prometheus.Incr(r.Config.Type+"-relay-producer", 0)

	var quit bool

	for {
		select {
		case msg := <-r.Config.RelayCh:
			queue = append(queue, msg)

			// Max queue size reached
			if len(queue) >= int(r.Config.BatchSize) {
				llog.Debugf("%d: max queue size reached - flushing!", id)

				r.flush(outboundCtx, conn, queue...)

				// Reset queue; since we passed by variadic, the underlying slice can be updated
				queue = make([]interface{}, 0)

				// Reset ticker (so time-based flush doesn't occur)
				flushTicker.Reset(QueueFlushInterval)

				// queue is empty, safe to quit
				if quit {
					llog.Debug("queue empty, worker exiting")
					r.removeWorker(id)
					return
				}
			}

		case <-flushTicker.C:
			if len(queue) != 0 {
				llog.Debugf("%d: flush ticker hit and queue not empty - flushing!", id)

				r.flush(outboundCtx, conn, queue...)

				// Reset queue; same as above - safe to delete queue contents
				queue = make([]interface{}, 0)
			}

			// queue is empty, safe to quit
			if quit {
				llog.Debug("queue empty, worker exiting")
				r.removeWorker(id)
				return
			}
		case <-shutdownCtx.Done():
			if quit == true {
				// Prevent log spam
				time.Sleep(time.Millisecond * 50)
				continue
			}
			llog.Debug("Shutdown signal received")

			// If queue is empty, quit immediately
			if len(queue) == 0 {
				llog.Debug("queue empty, worker exiting")
				r.removeWorker(id)
				return
			}

			quit = true
		}
	}
}

func (r *Relay) flush(ctx context.Context, conn *grpc.ClientConn, messages ...interface{}) {
	if len(messages) < 1 {
		r.log.Error("asked to flush empty message queue - bug?")
		return
	}

	// We only care about the first message since plumber can only be using
	// one message bus type at a time

	var err error

	// TODO: Need to get away from the switch type flow ~ds 09.11.21

	switch v := messages[0].(type) {
	case *sqsTypes.RelayMessage:
		r.log.Debugf("flushing %d sqs message(s)", len(messages))
		err = r.handleSQS(ctx, conn, messages)
	//case *rabbitTypes.RelayMessage:
	//	r.log.Debugf("flushing %d rabbit message(s)", len(messages))
	//	err = r.handleRabbit(ctx, conn, messages)
	case *kafkaTypes.RelayMessage:
		r.log.Debugf("flushing %d kafka message(s)", len(messages))
		err = r.handleKafka(ctx, conn, messages)
	//case *azureTypes.RelayMessage:
	//	r.log.Debugf("flushing %d azure message(s)", len(messages))
	//	err = r.handleAzure(ctx, conn, messages)
	//case *gcpTypes.RelayMessage:
	//	r.log.Debugf("flushing %d gcp message(s)", len(messages))
	//	err = r.handleGCP(ctx, conn, messages)
	//case *mongoTypes.RelayMessage:
	//	r.log.Debugf("flushing %d mongo message(s)", len(messages))
	//	err = r.handleCDCMongo(ctx, conn, messages)
	//case *redisTypes.RelayMessage:
	//	r.log.Debugf("flushing %d redis-pubsub message(s)", len(messages))
	//	err = r.handleRedisPubSub(ctx, conn, messages)
	case *rstreamsTypes.RelayMessage:
		r.log.Debugf("flushing %d redis-streams message(s)", len(messages))
		err = r.handleRedisStreams(ctx, conn, messages)
	case *postgresTypes.RelayMessage:
		r.log.Debugf("flushing %d cdc-postgres message(s)", len(messages))
		err = r.handleCdcPostgres(ctx, conn, messages)
	//case *mqttTypes.RelayMessage:
	//	r.log.Debugf("flushing %d mqtt message(s)", len(messages))
	//	err = r.handleMQTT(ctx, conn, messages)
	//case *nsqTypes.RelayMessage:
	//	r.log.Debugf("flushing %d nsq message(s)", len(messages))
	//	err = r.handleNSQ(ctx, conn, messages)
	default:
		r.log.WithField("type", v).Error("received unknown message type - skipping")
		return
	}

	if err != nil {
		r.log.WithField("err", err).Error("unable to handle message")
		return
	}

	numMsgs := len(messages)
	prometheus.Incr(r.Config.Type+"-relay-producer", numMsgs)
	prometheus.IncrPromCounter("plumber_relay_total", numMsgs)
}

// CallWithRetry will retry a GRPC call until it succeeds or reaches a maximum number of retries defined by MaxGRPCRetries
func (r *Relay) CallWithRetry(ctx context.Context, method string, publish func(ctx context.Context) error) error {
	var err error

	for i := 1; i <= MaxGRPCRetries; i++ {
		err = publish(ctx)
		if err != nil {
			prometheus.IncrPromCounter("plumber_grpc_errors", 1)

			// Paused collection, retries will fail, exit early
			if strings.Contains(err.Error(), "collection is paused") {
				return err
			}
			r.log.Debugf("unable to complete %s call [retry %d/%d]", method, i, 5)
			time.Sleep(GRPCRetrySleep)
			continue
		}
		r.log.Debugf("successfully handled %s message", strings.Replace(method, "Add", "", 1))
		return nil
	}

	return fmt.Errorf("unable to complete %s call [reached max retries (%d)]: %s", method, MaxGRPCRetries, err)
}

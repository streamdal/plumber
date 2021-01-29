package relay

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	sqsTypes "github.com/batchcorp/plumber/backends/aws-sqs/types"
	azureTypes "github.com/batchcorp/plumber/backends/azure/types"
	gcpTypes "github.com/batchcorp/plumber/backends/gcp-pubsub/types"
	kafkaTypes "github.com/batchcorp/plumber/backends/kafka/types"
	rabbitTypes "github.com/batchcorp/plumber/backends/rabbitmq/types"
	"github.com/batchcorp/plumber/stats"
)

const (
	DefaultNumWorkers = 10

	QueueFlushInterval = 10 * time.Second
	DefaultBatchSize   = 100 // number of messages to batch
)

type Relay struct {
	Config *Config
	log    *logrus.Entry
}

type Config struct {
	Token       string
	GRPCAddress string
	NumWorkers  int
	BatchSize   int
	RelayCh     chan interface{}
	DisableTLS  bool
	Timeout     time.Duration // general grpc timeout (used for all grpc calls)
}

func New(relayCfg *Config) (*Relay, error) {
	if err := validateConfig(relayCfg); err != nil {
		return nil, errors.Wrap(err, "unable to complete relay config validation")
	}

	// Verify grpc connection & token
	if err := TestConnection(relayCfg); err != nil {
		return nil, errors.Wrap(err, "unable to complete connection test")
	}

	// JSON formatter for log output if not running in a TTY - colors are fun!
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	return &Relay{
		Config: relayCfg,
		log:    logrus.WithField("pkg", "relay"),
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("Relay config cannot be nil")
	}

	if cfg.Token == "" {
		return errors.New("Token cannot be empty")
	}

	if cfg.GRPCAddress == "" {
		return errors.New("GRPCAddress cannot be empty")
	}

	if cfg.RelayCh == nil {
		return errors.New("RelayCh cannot be nil")
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

func (r *Relay) StartWorkers() error {
	for i := 0; i != r.Config.NumWorkers; i++ {
		r.log.WithField("workerId", i).Debug("starting worker")

		conn, ctx, err := NewConnection(r.Config.GRPCAddress, r.Config.Token, r.Config.Timeout, r.Config.DisableTLS, true)
		if err != nil {
			return fmt.Errorf("unable to create new gRPC connection for worker %d: %s", i, err)
		}

		go r.Run(i, conn, ctx)
	}

	return nil
}

func (r *Relay) Run(id int, conn *grpc.ClientConn, ctx context.Context) {
	llog := r.log.WithField("relayId", id)

	llog.Debug("Relayer started")

	queue := make([]interface{}, 0)

	// This functions as an escape-vale -- if we are pumping messages *REALLY*
	// fast - we will hit max queue size; if we are pumping messages slowly,
	// the ticker will be hit and the queue will be flushed, regardless of size.
	flushTicker := time.NewTicker(QueueFlushInterval)

	for {
		select {
		case msg := <-r.Config.RelayCh:
			queue = append(queue, msg)

			// Max queue size reached
			if len(queue) >= r.Config.BatchSize {
				r.log.Debugf("%d: max queue size reached - flushing!", id)

				r.flush(ctx, conn, queue...)

				// Reset queue; since we passed by variadic, the underlying slice can be updated
				queue = make([]interface{}, 0)

				// Reset ticker (so time-based flush doesn't occur)
				flushTicker.Reset(QueueFlushInterval)
			}
		case <-flushTicker.C:
			if len(queue) != 0 {
				r.log.Debugf("%d: flush ticker hit and queue not empty - flushing!", id)

				r.flush(ctx, conn, queue...)

				// Reset queue; same as above - safe to delete queue contents
				queue = make([]interface{}, 0)
			}
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
	var relayType string

	switch v := messages[0].(type) {
	case *sqsTypes.RelayMessage:
		r.log.Debugf("Run() received AWS SQS message %+v", v)
		relayType = "sqs"
		err = r.handleSQS(ctx, conn, v)
	case *rabbitTypes.RelayMessage:
		r.log.Debugf("Run() received rabbit message %+v", v)
		relayType = "rabbit"
		err = r.handleRabbit(ctx, conn, messages)
	case *kafkaTypes.RelayMessage:
		r.log.Debugf("Run() received kafka message %+v", v)
		relayType = "kafka"
		err = r.handleKafka(ctx, conn, messages)
	case *azureTypes.RelayMessage:
		r.log.Debugf("Run() received azure message %+v", v)
		relayType = "azure"
		err = r.handleAzure(ctx, conn, v)
	case *gcpTypes.RelayMessage:
		r.log.Debugf("Run() received GCP pubsub message %+v", v)
		relayType = "gcp"
		err = r.handleGCP(ctx, conn, v)
	default:
		r.log.WithField("type", v).Error("received unknown message type - skipping")
		return
	}

	if err != nil {
		r.log.WithField("err", err).Error("unable to handle message")
		return
	}

	stats.Incr(relayType+"-relay-producer", len(messages))
}

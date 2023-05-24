package natty

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	DefaultMaxMsgs           = 10_000
	DefaultFetchSize         = 100
	DefaultFetchTimeout      = time.Second * 1
	DefaultDeliverPolicy     = nats.DeliverLastPolicy
	DefaultSubBatchSize      = 256
	DefaultWorkerIdleTimeout = time.Minute
	DefaultPublishTimeout    = time.Second * 5 // TODO: figure out a good value for this
)

var (
	ErrEmptyStreamName   = errors.New("StreamName cannot be empty")
	ErrEmptyConsumerName = errors.New("ConsumerName cannot be empty")
	ErrEmptySubject      = errors.New("Subject cannot be empty")
)

type Mode int

type INatty interface {
	// Consume subscribes to given subject and executes callback every time a
	// message is received. Consumed messages must be explicitly ACK'd or NAK'd.
	//
	// This is a blocking call; cancellation should be performed via the context.
	Consume(ctx context.Context, cfg *ConsumerConfig, cb func(ctx context.Context, msg *nats.Msg) error) error

	// Publish publishes a single message with the given subject; this method
	// will perform automatic batching as configured during `natty.New(..)`
	Publish(ctx context.Context, subject string, data []byte)

	// DeletePublisher shuts down a publisher and deletes it from the internal publisherMap
	DeletePublisher(ctx context.Context, id string) bool

	// CreateStream creates a new stream if it does not exist
	CreateStream(ctx context.Context, name string, subjects []string) error

	// DeleteStream deletes an existing stream
	DeleteStream(ctx context.Context, name string) error

	// CreateConsumer creates a new consumer if it does not exist
	CreateConsumer(ctx context.Context, streamName, consumerName string, filterSubject ...string) error

	// DeleteConsumer deletes an existing consumer
	DeleteConsumer(ctx context.Context, consumerName, streamName string) error

	// NATS key/value Get/Put/Delete/Update functionality operates on "buckets"
	// that are exposed via a 'KeyValue' instance. To simplify our interface,
	// our Put method will automatically create the bucket if it does not already
	// exist. Get() and Delete() will not automatically create a bucket.
	//
	// If your functionality is creating many ephemeral buckets, you may want to
	// delete the bucket after you are done via DeleteBucket().

	// Get will fetch the value for a given bucket and key. Will NOT auto-create
	// bucket if it does not exist.
	Get(ctx context.Context, bucket string, key string) ([]byte, error)

	// Create will attempt to create a key in KV. It will return an error if
	// the key already exists. Will auto-create the bucket if it does not
	// already exist.
	Create(ctx context.Context, bucket string, key string, data []byte, keyTTL ...time.Duration) error

	// Put will put a new value for a given bucket and key. Will auto-create
	// the bucket if it does not already exist.
	Put(ctx context.Context, bucket string, key string, data []byte, ttl ...time.Duration) error

	// Delete will delete a key from a given bucket. Will no-op if the bucket
	// or key does not exist.
	Delete(ctx context.Context, bucket string, key string) error

	// CreateBucket will attempt to create a new bucket. Will return an error if
	// bucket already exists.
	CreateBucket(ctx context.Context, bucket string, ttl time.Duration, replicas int, description ...string) error

	// DeleteBucket will delete the specified bucket
	DeleteBucket(ctx context.Context, bucket string) error

	// WatchBucket returns an instance of nats.KeyWatcher for the given bucket
	WatchBucket(ctx context.Context, bucket string) (nats.KeyWatcher, error)

	// Keys will return all of the keys in a bucket (empty slice if none found)
	Keys(ctx context.Context, bucket string) ([]string, error)

	// AsLeader enables simple leader election by using NATS k/v functionality.
	//
	// AsLeader will execute opts.Func if and only if the node executing AsLeader
	// acquires leader role. It will continue executing opts.Func until it loses
	// leadership and another node becomes leader.
	AsLeader(ctx context.Context, opts AsLeaderConfig, f func() error) error

	// HaveLeader returns bool indicating whether node-name in given cfg is the
	// leader for the cfg.Bucket and cfg.Key
	HaveLeader(ctx context.Context, nodeName, bucketName, keyName string) bool

	// GetLeader returns the current leader for a given bucket and key
	GetLeader(ctx context.Context, bucketName, keyName string) (string, error)
}

type Config struct {
	// NatsURL defines the NATS urls the library will attempt to connect to. Iff
	// first URL fails, we will try to connect to the next one. Only fail if all
	// URLs fail.
	NatsURL []string

	// MaxMsgs defines the maximum number of messages a stream will contain.
	MaxMsgs int64

	// FetchSize defines the number of messages to fetch from the stream during
	// a single Fetch() call.
	FetchSize int

	// FetchTimeout defines how long a Fetch() call will wait to attempt to reach
	// defined FetchSize before continuing.
	FetchTimeout time.Duration

	// DeliverPolicy defines the policy the library will use to deliver messages.
	// Default: DeliverLastPolicy which will deliver from the last message that
	// the consumer has seen.
	DeliverPolicy nats.DeliverPolicy

	// Logger allows you to inject a logger into the library. Optional.
	Logger Logger

	// Whether to use TLS
	UseTLS bool

	// TLS CA certificate file
	TLSCACertFile string

	// TLS client certificate file
	TLSClientCertFile string

	// TLS client key file
	TLSClientKeyFile string

	// Do not perform server certificate checks
	TLSSkipVerify bool

	// PublishBatchSize is how many messages to async publish at once
	// Default: 256
	PublishBatchSize int

	// ServiceShutdownContext is used by main() to shutdown services before application termination
	ServiceShutdownContext context.Context

	// MainShutdownFunc is triggered by watchForShutdown() after all publisher queues are exhausted
	// and is used to trigger shutdown of APIs and then main()
	MainShutdownFunc context.CancelFunc

	// WorkerIdleTimeout determines how long to keep a publish worker alive if no activity
	WorkerIdleTimeout time.Duration

	// PublishTimeout is how long to wait for a batch of async publish calls to be ACK'd
	PublishTimeout time.Duration

	// PublishErrorCh will receive any
	PublishErrorCh chan *PublishError
}

// ConsumerConfig is used to pass configuration options to Consume()
type ConsumerConfig struct {
	// Subject is the subject to consume off of a stream
	Subject string

	// StreamName is the name of JS stream to consume from.
	// This should first be created with CreateStream()
	StreamName string

	// ConsumerName is the consumer that was made with CreateConsumer()
	ConsumerName string

	// Looper is optional, if none is provided, one will be created
	Looper director.Looper

	// ErrorCh is used to retrieve any errors returned during asynchronous publishing
	// If nil, errors will only be logged
	ErrorCh chan error
}

type Publisher struct {
	Subject     string
	QueueMutex  *sync.RWMutex
	Queue       []*message
	Natty       *Natty
	IdleTimeout time.Duration
	looper      director.Looper

	// ErrorCh is optional. It will receive async publish errors if specified
	// Otherwise errors will only be logged
	ErrorCh chan *PublishError

	// PublisherContext is used to close a specific publisher
	PublisherContext context.Context

	// PublisherCancel is used to cancel a specific publisher's context
	PublisherCancel context.CancelFunc

	// ServiceShutdownContext is used by main() to shutdown services before application termination
	ServiceShutdownContext context.Context

	log Logger
}

// message is a convenience struct to hold message data for a batch
type message struct {
	Subject string
	Value   []byte
}

// PublishError is a wrapper struct used to return errors to code that occur during async batch publishes
type PublishError struct {
	Subject string
	Message error
}

type Natty struct {
	*Config
	nc             *nats.Conn
	js             nats.JetStreamContext
	consumerLooper director.Looper
	kvMap          *KeyValueMap
	kvMutex        *sync.RWMutex
	publisherMutex *sync.RWMutex
	publisherMap   map[string]*Publisher

	// key == asLeaderKey(bucket, key)
	// value == node name
	leaderMap   map[string]string
	leaderMutex *sync.RWMutex
	log         Logger
}

func New(cfg *Config) (*Natty, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	var connected bool
	var nc *nats.Conn
	var err error
	var tlsConfig *tls.Config

	if cfg.UseTLS {
		tlsConfig, err = GenerateTLSConfig(cfg.TLSCACertFile, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSSkipVerify)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create TLS config")
		}
	}

	// Attempt to connect
	for _, address := range cfg.NatsURL {
		if cfg.UseTLS {
			nc, err = nats.Connect(address, nats.Secure(tlsConfig))
		} else {
			nc, err = nats.Connect(address)
		}

		if err != nil {
			fmt.Printf("unable to connect to '%s': %s\n", address, err)

			continue
		}

		connected = true
		break
	}

	if !connected {
		return nil, errors.Wrap(err, "failed to connect to NATS")
	}

	// Create js context
	js, err := nc.JetStream()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create jetstream context")
	}

	n := &Natty{
		nc:     nc,
		js:     js,
		Config: cfg,
		kvMap: &KeyValueMap{
			rwMutex: &sync.RWMutex{},
			kvMap:   make(map[string]nats.KeyValue),
		},
		publisherMutex: &sync.RWMutex{},
		publisherMap:   make(map[string]*Publisher),
		leaderMap:      make(map[string]string),
		leaderMutex:    &sync.RWMutex{},
	}

	// Inject logger (if provided)
	n.log = cfg.Logger

	if n.log == nil {
		n.log = &NoOpLogger{}
	}

	return n, nil
}

func (n *Natty) DeleteStream(ctx context.Context, name string) error {
	span, _ := tracer.StartSpanFromContext(ctx, "natty.DeleteStream")
	defer span.Finish()

	if err := n.js.DeleteStream(name); err != nil {
		err = errors.Wrap(err, "unable to delete stream")
		span.SetTag("error", err)
		return err
	}

	return nil
}

func (n *Natty) CreateStream(ctx context.Context, name string, subjects []string) error {
	span, _ := tracer.StartSpanFromContext(ctx, "natty.CreateStream")
	defer span.Finish()

	// Check if stream exists
	_, err := n.js.StreamInfo(name)
	if err == nil {
		// We have a stream already, nothing else to do
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		err = errors.Wrap(err, "unable to create stream")
		span.SetTag("error", err)
		return err
	}

	_, err = n.js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Retention: nats.LimitsPolicy,   // Limit to age
		MaxAge:    time.Hour * 24 * 30, // 30 days max retention
		Storage:   nats.FileStorage,    // Store on disk

	})
	if err != nil {
		err = errors.Wrap(err, "unable to create stream")
		span.SetTag("error", err)
		return err
	}

	return nil
}

func GenerateTLSConfig(caCertFile, clientKeyFile, clientCertFile string, tlsSkipVerify bool) (*tls.Config, error) {
	if caCertFile == "" && clientKeyFile == "" && clientCertFile == "" {
		return &tls.Config{
			InsecureSkipVerify: tlsSkipVerify,
		}, nil
	}

	var certpool *x509.CertPool

	if caCertFile != "" {
		certpool = x509.NewCertPool()

		pemCerts, err := ioutil.ReadFile(caCertFile)
		if err == nil {
			certpool.AppendCertsFromPEM(pemCerts)
		}
	}

	var cert tls.Certificate

	if clientKeyFile != "" && clientCertFile != "" {
		var err error

		// Import client certificate/key pair
		cert, err = tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to load ssl keypair")
		}

		// Just to print out the client certificate..
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse certificate")
		}

	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: tlsSkipVerify,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

func (n *Natty) CreateConsumer(ctx context.Context, streamName, consumerName string, filterSubject ...string) error {
	span, _ := tracer.StartSpanFromContext(ctx, "natty.CreateConsumer")
	defer span.Finish()

	var filter string

	if len(filterSubject) > 0 {
		filter = filterSubject[0]
	}

	if _, err := n.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: filter,
	}); err != nil {
		err = errors.Wrap(err, "unable to create consumer")
		span.SetTag("error", err)
		return err
	}

	return nil
}

func (n *Natty) DeleteConsumer(ctx context.Context, consumerName, streamName string) error {
	span, _ := tracer.StartSpanFromContext(ctx, "natty.CreateConsumer")
	defer span.Finish()

	if err := n.js.DeleteConsumer(streamName, consumerName); err != nil {
		err = errors.Wrap(err, "unable to delete consumer")
		span.SetTag("error", err)
		return err
	}

	return nil
}

// Consume will create a durable consumer and consume messages from the configured stream
func (n *Natty) Consume(ctx context.Context, cfg *ConsumerConfig, f func(ctx context.Context, msg *nats.Msg) error) error {
	if err := validateConsumerConfig(cfg); err != nil {
		return errors.Wrap(err, "invalid consumer config")
	}

	sub, err := n.js.PullSubscribe(cfg.Subject, cfg.ConsumerName)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			n.log.Errorf("unable to unsubscribe from (stream: '%s', subj: '%s'): %s",
				cfg.StreamName, cfg.Subject, err)
		}
	}()

	var quit bool

	cfg.Looper.Loop(func() error {
		// This is needed to prevent context flood in case .Quit() wasn't picked
		// up quickly enough by director
		if quit {
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		msgs, err := sub.Fetch(n.FetchSize, nats.Context(ctx))
		if err != nil {
			if err == context.Canceled {
				n.log.Debugf("context canceled (stream: %s, subj: %s)",
					cfg.StreamName, cfg.Subject)

				cfg.Looper.Quit()
				quit = true

				return nil
			}

			if err == context.DeadlineExceeded {
				// No problem, context timed out - try again
				return nil
			}

			n.report(cfg.ErrorCh, fmt.Errorf("unable to fetch messages from (stream: '%s', subj: '%s'): %s",
				cfg.StreamName, cfg.Subject, err))

			return nil
		}

		for _, m := range msgs {
			if err := f(ctx, m); err != nil {
				n.report(cfg.ErrorCh, fmt.Errorf("callback func failed during message processing (stream: '%s', subj: '%s', msg: '%s'): %s",
					cfg.StreamName, cfg.Subject, m.Data, err))
			}
		}

		return nil
	})

	n.log.Debugf("consumer exiting (stream: %s, subj: %s)", cfg.StreamName, cfg.Subject)

	return nil
}

func (n *Natty) report(errorCh chan error, err error) {
	n.log.Error(err)

	if errorCh != nil {
		// Write the err in a goroutine to avoid block in case chan is full
		go func() {
			select {
			case errorCh <- err:
			default:
				n.log.Warnf("consumer error channel is full; discarding error")
			}
		}()
	}

	n.log.Error(err)
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if len(cfg.NatsURL) == 0 {
		return errors.New("NatsURL cannot be empty")
	}

	if cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = DefaultMaxMsgs
	}

	if cfg.FetchSize == 0 {
		cfg.FetchSize = DefaultFetchSize
	}

	if cfg.FetchTimeout == 0 {
		cfg.FetchTimeout = DefaultFetchTimeout
	}

	if cfg.DeliverPolicy == 0 {
		cfg.DeliverPolicy = DefaultDeliverPolicy
	}

	if cfg.PublishBatchSize == 0 {
		cfg.PublishBatchSize = DefaultSubBatchSize
	}
	if cfg.WorkerIdleTimeout == 0 {
		cfg.WorkerIdleTimeout = DefaultWorkerIdleTimeout
	}

	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = DefaultPublishTimeout
	}

	if cfg.ServiceShutdownContext == nil {
		ctx, _ := context.WithCancel(context.Background())
		cfg.ServiceShutdownContext = ctx
	}

	return nil
}

func validateConsumerConfig(cfg *ConsumerConfig) error {
	if cfg.StreamName == "" {
		return ErrEmptyStreamName
	}

	if cfg.ConsumerName == "" {
		return ErrEmptyConsumerName
	}

	if cfg.Subject == "" {
		return ErrEmptySubject
	}

	if cfg.Looper == nil {
		cfg.Looper = director.NewFreeLooper(director.FOREVER, cfg.ErrorCh)
	}

	return nil
}

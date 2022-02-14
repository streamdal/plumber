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
)

const (
	DefaultMaxMsgs       = 10_000
	DefaultFetchSize     = 100
	DefaultFetchTimeout  = time.Second * 1
	DefaultDeliverPolicy = nats.DeliverLastPolicy
)

type Mode int

type INatty interface {
	// Consume subscribes to given subject and executes callback every time a
	// message is received. This is a blocking call; cancellation should be
	// performed via the context.
	Consume(ctx context.Context, subj string, errorCh chan error, cb func(ctx context.Context, msg *nats.Msg) error) error

	// Publish publishes a single message with the given subject
	Publish(ctx context.Context, subject string, data []byte) error

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

	// Put will put a new value for a given bucket and key. Will auto-create
	// the bucket if it does not already exist.
	Put(ctx context.Context, bucket string, key string, data []byte, ttl ...time.Duration) error

	// Delete will delete a key from a given bucket. Will no-op if the bucket
	// or key does not exist.
	Delete(ctx context.Context, bucket string, key string) error
}

type Config struct {
	// NoConsumer will prevent the NATS stream + consumer from being created
	// during library initialization via New().
	NoConsumer bool

	// NatsURL defines the NATS urls the library will attempt to connect to. Iff
	// first URL fails, we will try to connect to the next one. Only fail if all
	// URLs fail.
	NatsURL []string

	// StreamName is the name of the stream the library will attempt to create.
	// Stream creation will NOOP if the stream already exists.
	StreamName string

	// StreamSubjects defines the subjects that the stream will listen to.
	StreamSubjects []string

	// ConsumerName defines the name of the consumer the library will create.
	// NOTE: The library *always* creates durable consumers.
	ConsumerName string

	// ConsumerFilterSubject is useful for wildcard streams - it will ensure
	// that a consumer only receives messages that match the subject filter
	ConsumerFilterSubject string

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

	// ConsumerLooper allows you to inject a looper into the library. Optional.
	ConsumerLooper director.Looper

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
}

type Natty struct {
	js             nats.JetStreamContext
	consumerLooper director.Looper
	config         *Config
	kvMap          *KeyValueMap
	kvMutex        *sync.RWMutex
	log            Logger
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
		js:     js,
		config: cfg,
		kvMap: &KeyValueMap{
			rwMutex: &sync.RWMutex{},
			kvMap:   make(map[string]nats.KeyValue),
		},
	}

	// Inject looper (if provided)
	n.consumerLooper = cfg.ConsumerLooper

	if n.consumerLooper == nil {
		n.consumerLooper = director.NewFreeLooper(director.FOREVER, make(chan error, 1))
	}

	// Inject logger (if provided)
	n.log = cfg.Logger

	if n.log == nil {
		n.log = &NoOpLogger{}
	}

	if cfg.NoConsumer {
		return n, nil
	}

	// Create stream
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: cfg.StreamSubjects,
		MaxMsgs:  cfg.MaxMsgs,
	}); err != nil {
		return nil, errors.Wrap(err, "unable to create stream")
	}

	// Create consumer
	if _, err := js.AddConsumer(cfg.StreamName, &nats.ConsumerConfig{
		Durable:       cfg.ConsumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: cfg.ConsumerFilterSubject,
	}); err != nil {
		return nil, errors.Wrap(err, "unable to create consumer")
	}

	return n, nil
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

// Consume will create a durable consumer and consume messages from the configured stream
func (n *Natty) Consume(ctx context.Context, subj string, errorCh chan error, f func(ctx context.Context, msg *nats.Msg) error) error {
	if n.config.NoConsumer {
		return errors.New("consumer disabled")
	}

	sub, err := n.js.PullSubscribe(subj, n.config.ConsumerName)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			n.log.Errorf("unable to unsubscribe from (stream: '%s', subj: '%s'): %s",
				n.config.StreamName, subj, err)
		}
	}()

	var quit bool

	n.consumerLooper.Loop(func() error {
		// This is needed to prevent context flood in case .Quit() wasn't picked
		// up quickly enough by director
		if quit {
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		msgs, err := sub.Fetch(n.config.FetchSize, nats.Context(ctx))
		if err != nil {
			if err == context.Canceled {
				n.log.Debugf("context canceled (stream: %s, subj: %s)",
					n.config.StreamName, subj)

				n.consumerLooper.Quit()
				quit = true

				return nil
			}

			if err == context.DeadlineExceeded {
				// No problem, context timed out - try again
				return nil
			}

			n.report(errorCh, fmt.Errorf("unable to fetch messages from (stream: '%s', subj: '%s'): %s",
				n.config.StreamName, subj, err))

			return nil
		}

		for _, v := range msgs {
			if err := f(ctx, v); err != nil {
				n.report(errorCh, fmt.Errorf("callback func failed during message processing (stream: '%s', subj: '%s', msg: '%s'): %s",
					n.config.StreamName, subj, v.Data, err))
			}
		}

		return nil
	})

	n.log.Debugf("consumer exiting (stream: %s, subj: %s)", n.config.StreamName, subj)

	return nil
}

func (n *Natty) Publish(ctx context.Context, subj string, msg []byte) error {
	if _, err := n.js.Publish(subj, msg, nats.Context(ctx)); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	return nil
}

func (n *Natty) report(errorCh chan error, err error) {
	if errorCh != nil {
		// Write the err in a goroutine to avoid block in case chan is full
		go func() {
			errorCh <- err
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

	if !cfg.NoConsumer {
		if cfg.ConsumerName == "" {
			return errors.New("ConsumerName cannot be empty")
		}

		if cfg.StreamName == "" {
			return errors.New("StreamName cannot be empty")
		}

		if len(cfg.StreamSubjects) == 0 {
			return errors.New("StreamSubjects cannot be empty")
		}
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

	return nil
}

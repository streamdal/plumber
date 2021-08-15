package nsq

import (
	context "context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/batchcorp/plumber/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

var (
	ErrMissingAddress = errors.New("you must specify either --nsqd-address or --lookupd-address")
	ErrChooseAddress  = errors.New("you must specify either --nsqd-address or --lookupd-address, not both")
	ErrMissingTLSKey  = errors.New("--tls-client-key-file cannot be blank if using TLS")
	ErrMissingTlsCert = errors.New("--tls-client-cert-file cannot be blank if using TLS")
	ErrMissingTLSCA   = errors.New("--tls-ca-file cannot be blank if using TLS")
)

// NSQ encapsulates options for calling Read() and Write() methods
type NSQ struct {
	Options *options.Options

	msgDesc  *desc.MessageDescriptor
	consumer *nsq.Consumer
	producer *nsq.Producer
	log      *NSQLogger
}

func New(opts *options.Options) (*NSQ, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	cfg, err := getNSQConfig(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate nsq config")
	}

	logger := &NSQLogger{
		Entry: logrus.WithField("backend", "nsq"),
	}

	logLevel := nsq.LogLevelError
	if opts.Debug {
		logLevel = nsq.LogLevelDebug
	}

	consumer, err := nsq.NewConsumer(opts.NSQ.Topic, opts.NSQ.Channel, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Could not start NSQ consumer")
	}

	// Connect to correct server. Reading can be done directly from an NSQD server
	// or let lookupd find the correct one.
	if opts.NSQ.NSQLookupDAddress != "" {
		if err := consumer.ConnectToNSQLookupd(opts.NSQ.NSQLookupDAddress); err != nil {
			return nil, errors.Wrap(err, "could not connect to nsqlookupd")
		}
	} else {
		if err := consumer.ConnectToNSQD(opts.NSQ.NSQDAddress); err != nil {
			return nil, errors.Wrap(err, "could not connect to nsqd")
		}
	}

	producer, err := nsq.NewProducer(opts.NSQ.NSQDAddress, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start NSQ producer")
	}

	consumer.SetLogger(logger, logLevel)
	producer.SetLogger(logger, logLevel)

	return &NSQ{
		Options:  opts,
		consumer: consumer,
		producer: producer,
		log:      logger,
	}, nil
}

func (n *NSQ) Close(ctx context.Context) error {
	// TODO: Wrap Stop()'s in ctx to support timeout

	if n.consumer != nil {
		n.consumer.Stop()
	}

	if n.producer != nil {
		n.producer.Stop()
	}

	n.consumer = nil
	n.producer = nil

	return nil
}

func (n *NSQ) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (n *NSQ) Dynamic(ctx context.Context) error {
	return types.UnsupportedFeatureErr
}

func (n *NSQ) Lag(ctx context.Context) (*types.Lag, error) {
	return nil, types.UnsupportedFeatureErr
}

// getNSQConfig returns the config needed for creating a new NSQ consumer or producer
func getNSQConfig(opts *options.Options) (*nsq.Config, error) {
	config := nsq.NewConfig()
	config.ClientID = opts.NSQ.ClientID

	if opts.NSQ.AuthSecret != "" {
		config.AuthSecret = opts.NSQ.AuthSecret
	}

	if opts.NSQ.UseTLS || opts.NSQ.TLSClientCertFile != "" {
		tlsConfig, err := generateTLSConfig(opts)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate TLS config")
		}

		config.TlsConfig = tlsConfig
	}

	return config, nil
}

// generateTLSConfig generates necessary TLS config for Dialing to an NSQ server
func generateTLSConfig(opts *options.Options) (*tls.Config, error) {
	// No client certs
	if opts.NSQ.TLSClientCertFile == "" {
		return &tls.Config{
			InsecureSkipVerify: opts.NSQ.InsecureTLS,
		}, nil
	}

	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(opts.NSQ.TLSCAFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(opts.NSQ.TLSClientCertFile, opts.NSQ.TLSClientKeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load ssl keypair")
	}

	// Just to print out the client certificate..
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse certificate")
	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: opts.NSQ.InsecureTLS,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("opts cannot be nil")
	}

	if opts.NSQ == nil {
		return errors.New("NSQ opts cannot be nil")
	}

	return nil
}

// NSQLogger wraps logrus and implements the Output() method so we can satisfy the interface
// requirements for NSQ's logger
type NSQLogger struct {
	*logrus.Entry
}

// Output writes an NSQ log message via logrus
func (n *NSQLogger) Output(_ int, s string) error {
	n.Info(s)
	return nil
}

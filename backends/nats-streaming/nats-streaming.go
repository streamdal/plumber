package nats_streaming

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

const (
	BackendName = "nats-streaming"
)

type NatsStreaming struct {
	Options *options.Options

	msgDesc    *desc.MessageDescriptor
	client     *nats.Conn
	stanClient stan.Conn
	log        *logrus.Entry
}

func New(opts *options.Options) (*NatsStreaming, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, stanClient, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new nats client")
	}

	return &NatsStreaming{
		Options:    opts,
		client:     client,
		stanClient: stanClient,
		log:        logrus.WithField("backend", "nats_streaming"),
	}, nil
}

func (n *NatsStreaming) Name() string {
	return BackendName
}

func (n *NatsStreaming) Close(ctx context.Context) error {
	if n.client != nil {
		n.client.Close()
	}

	if n.stanClient != nil {
		if err := n.stanClient.Close(); err != nil {
			return errors.Wrap(err, "unable to close stan client")
		}
	}

	n.client = nil
	n.stanClient = nil

	return nil
}

func (n *NatsStreaming) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (n *NatsStreaming) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func (n *NatsStreaming) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

// NewClient creates a new Nats client connection
func newClient(opts *options.Options) (*nats.Conn, stan.Conn, error) {
	uri, err := url.Parse(opts.Nats.Address)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in the address
	var creds nats.Option
	if opts.Nats.CredsFile != "" {
		creds = nats.UserCredentials(opts.Nats.CredsFile)
	}

	var connErr error
	var client *nats.Conn

	if uri.Scheme != "tls" {
		// Insecure connection
		client, connErr = nats.Connect(opts.Nats.Address, creds)
	} else {
		// TLS Secured connection
		tlsConfig, err := generateTLSConfig(opts)
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to generate TLS config")
		}

		client, connErr = nats.Connect(opts.Nats.Address, nats.Secure(tlsConfig), creds)
	}

	if connErr != nil {
		return nil, nil, errors.Wrap(err, "unable to create connection")
	}

	stanClient, err := stan.Connect(opts.NatsStreaming.ClusterID, opts.NatsStreaming.ClientID, stan.NatsConn(client))
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create stan client")
	}

	return client, stanClient, nil
}

func generateTLSConfig(opts *options.Options) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(opts.Nats.TLSCAFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(opts.Nats.TLSClientCertFile, opts.Nats.TLSClientKeyFile)
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
		InsecureSkipVerify: opts.Nats.InsecureTLS,
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

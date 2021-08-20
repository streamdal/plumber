package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

const (
	BackendName = "nats"
)

var (
	errMissingSubject = errors.New("you must specify a subject to publish to")
)

type Nats struct {
	Options *options.Options

	client *nats.Conn
	log    *logrus.Entry
}

func New(opts *options.Options) (*Nats, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new nats client")
	}

	return &Nats{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "nats"),
	}, nil
}

func (n *Nats) Name() string {
	return BackendName
}

func (n *Nats) Close(ctx context.Context) error {
	if n.client == nil {
		return nil
	}

	// TODO: Wrap in a context to support timeout
	n.client.Close()

	n.client = nil

	return nil
}

func (n *Nats) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (n *Nats) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func (n *Nats) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

// NewClient creates a new Nats client connection
func newClient(opts *options.Options) (*nats.Conn, error) {
	uri, err := url.Parse(opts.Nats.Address)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in the address
	var creds nats.Option

	if opts.Nats.CredsFile != "" {
		creds = nats.UserCredentials(opts.Nats.CredsFile)
	}

	if uri.Scheme != "tls" {
		// Insecure connection
		c, err := nats.Connect(opts.Nats.Address, creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to complete non-tls connection")
		}
		return c, nil
	}

	// TLS Secured connection
	tlsConfig, err := generateTLSConfig(opts)
	if err != nil {
		return nil, err
	}

	c, err := nats.Connect(opts.Nats.Address, nats.Secure(tlsConfig), creds)
	if err != nil {
		return nil, errors.Wrap(err, "unable to complete tls connection")
	}

	return c, nil
}

func generateTLSConfig(opts *options.Options) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	var cert tls.Certificate
	var err error

	if opts.Nats.TLSClientCertFile != "" {
		pemCerts, err := ioutil.ReadFile(opts.Nats.TLSCAFile)
		if err == nil {
			certpool.AppendCertsFromPEM(pemCerts)
		}

		// Import client certificate/key pair
		cert, err = tls.LoadX509KeyPair(opts.Nats.TLSClientCertFile, opts.Nats.TLSClientKeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to load ssl keypair")
		}
	} else if opts.Nats.TLSClientCertData != "" {
		certpool.AppendCertsFromPEM([]byte(opts.Nats.TLSClientCertData))

		cert, err = tls.X509KeyPair([]byte(opts.Nats.TLSClientCertData), []byte(opts.Nats.TLSClientKeyFile))
		if err != nil {
			return nil, errors.Wrap(err, "unable to load ssl keypair")
		}
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

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.Nats == nil {
		return errors.New("NATS options cannot be nil")
	}

	if opts.Nats.Subject == "" {
		return errMissingSubject
	}

	return nil
}

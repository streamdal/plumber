package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
)

const BackendName = "nats"

var ErrMissingSubject = errors.New("you must specify a subject to publish to")

type Nats struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.NatsConn

	Client *nats.Conn
	log    *logrus.Entry
}

func New(opts *opts.ConnectionOptions) (*Nats, error) {

	c, err := newClient(opts.GetNats())
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new Nats client")
	}

	return &Nats{
		connOpts: opts,
		connArgs: opts.GetNats(),
		Client:   c,
		log:      logrus.WithField("backend", "nats"),
	}, nil
}

func (n *Nats) Name() string {
	return BackendName
}

func (n *Nats) Close(_ context.Context) error {
	n.Client.Close()
	return nil
}

func (n *Nats) Test(_ context.Context) error {
	return types.NotImplementedErr
}

// newClient creates a new Nats client connection
func newClient(opts *args.NatsConn) (*nats.Conn, error) {
	uri, err := url.Parse(opts.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in the address
	var creds nats.Option
	if len(opts.UserCredentials) > 0 {
		creds = nats.UserCredentials(string(opts.UserCredentials))
	}

	if uri.Scheme != "tls" {
		// Insecure connection
		c, err := nats.Connect(opts.Dsn, creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
		return c, nil
	}

	// TLS Secured connection
	tlsConfig, err := generateTLSConfig(opts)
	if err != nil {
		return nil, err
	}

	c, err := nats.Connect(opts.Dsn, nats.Secure(tlsConfig), creds)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new nats client")
	}

	return c, nil
}

func generateTLSConfig(opts *args.NatsConn) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(string(opts.TlsCaCert))
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(string(opts.TlsClientCert), string(opts.TlsClientKey))
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
		InsecureSkipVerify: opts.InsecureTls,
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
	}, nil
}

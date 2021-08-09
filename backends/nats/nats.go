package nats

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"

	"github.com/jhump/protoreflect/desc"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

var (
	errMissingSubject = errors.New("you must specify a subject to publish to")
)

type Nats struct {
	Options *options.Options
	MsgDesc *desc.MessageDescriptor
	Client  *nats.Conn
	log     *logrus.Entry
}

// NewClient creates a new Nats client connection
func NewClient(opts *options.Options) (*nats.Conn, error) {
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
			return nil, errors.Wrap(err, "unable to create new nats client")
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
		return nil, errors.Wrap(err, "unable to create new nats client")
	}

	return c, nil
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

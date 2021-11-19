package nats_streaming

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "nats-streaming"

var (
	ErrMissingChannel    = errors.New("channel cannot be empty")
	ErrInvalidReadOption = errors.New("You may only specify one read option of --last, --all, --seq, --since")
)

type NatsStreaming struct {
	connOpts   *opts.ConnectionOptions
	connArgs   *args.NatsStreamingConn
	client     *nats.Conn
	stanClient stan.Conn
	log        *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*NatsStreaming, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	args := connOpts.GetNatsStreaming()

	uri, err := url.Parse(args.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in the address
	var creds nats.Option
	if len(args.UserCredentials) > 0 {
		creds, err = nkeys.ParseDecoratedJWT(args.UserCredentials)
		//creds = nats.UserCredentials(args.UserCredentials)
	}

	var natsClient *nats.Conn
	if uri.Scheme == "tls" {
		// TLS Secured connection
		tlsConfig, err := generateTLSConfig(args)
		if err != nil {
			return nil, err
		}

		natsClient, err = nats.Connect(args.Dsn, nats.Secure(tlsConfig), creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
	} else {
		// Plaintext connection
		natsClient, err = nats.Connect(args.Dsn, creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
	}

	stanClient, err := stan.Connect(args.ClusterId, args.ClientId, stan.NatsOptions())
	if err != nil {
		return nil, errors.Wrap(err, "could not create NATS subscription")
	}

	return &NatsStreaming{
		connOpts:   connOpts,
		connArgs:   args,
		client:     natsClient,
		stanClient: stanClient,
		log:        logrus.WithField("backend", BackendName),
	}, nil
}

func (n *NatsStreaming) Name() string {
	return BackendName
}

func (n *NatsStreaming) Close(_ context.Context) error {
	if err := n.stanClient.Close(); err != nil {
		return err
	}

	n.client.Close()

	return nil
}

func (n *NatsStreaming) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func generateTLSConfig(args *args.NatsStreamingConn) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	/*	pemCerts, err := ioutil.ReadFile(args.TlsCaCert)
		if err == nil {
		}
	*/
	certpool.AppendCertsFromPEM(args.TlsCaCert)
	// Import client certificate/key pair
	cert, err := tls.X509KeyPair(args.TlsClientCert, args.TlsClientKey)
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
		InsecureSkipVerify: args.InsecureTls,
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
	}, nil
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	if connOpts.GetNatsStreaming() == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

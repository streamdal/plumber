package nats

import (
	"context"
	"crypto/tls"
	"net/url"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
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
		log:      logrus.WithField("backend", BackendName),
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
func newClient(args *args.NatsConn) (*nats.Conn, error) {
	uri, err := url.Parse(args.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in the address
	// Credentials are in NKey format
	opts := make([]nats.Option, 0)
	if len(args.Nkey) > 0 {
		authOpts, err := util.GenerateNATSAuthNKey(args.Nkey)
		if err != nil {
			return nil, err
		}

		opts = append(opts, authOpts...)
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in with the DSN
	if len(args.UserCredentials) > 0 {
		authOpts, err := util.GenerateNATSAuthJWT(args.UserCredentials)
		if err != nil {
			return nil, err
		}

		opts = append(opts, authOpts...)
	}

	if uri.Scheme != "tls" && !args.TlsOptions.UseTls {
		// Insecure connection
		c, err := nats.Connect(args.Dsn, opts...)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
		return c, nil
	}

	// TLS Secured connection
	tlsConfig, err := util.GenerateTLSConfig(
		args.TlsOptions.TlsCaCert,
		args.TlsOptions.TlsClientCert,
		args.TlsOptions.TlsClientKey,
		args.TlsOptions.TlsSkipVerify,
		tls.NoClientCert,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to generate TLS Config")
	}

	opts = append(opts, nats.Secure(tlsConfig))

	c, err := nats.Connect(args.Dsn, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new nats client")
	}

	return c, nil
}

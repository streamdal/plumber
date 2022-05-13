package nats_jetstream

import (
	"context"
	"crypto/tls"
	"net/url"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const BackendName = "nats-jetstream"

var (
	ErrMissingStream = errors.New("--stream cannot be empty")
)

type NatsJetstream struct {
	connOpts *opts.ConnectionOptions
	client   *nats.Conn
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*NatsJetstream, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	args := connOpts.GetNatsJetstream()

	uri, err := url.Parse(args.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in with the DSN
	var creds nats.Option
	if len(args.UserCredentials) > 0 {
		if util.FileExists(args.UserCredentials) {
			creds = nats.UserCredentials(string(args.UserCredentials))
		} else {
			creds = func(o *nats.Options) error {
				o.UserJWT = func() (string, error) {
					return string(args.UserCredentials), nil
				}
				o.SignatureCB = nil
				return nil
			}
		}
	}

	var client *nats.Conn
	if uri.Scheme == "tls" || args.TlsOptions.UseTls {
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

		client, err = nats.Connect(args.Dsn, nats.Secure(tlsConfig), creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
	} else {
		// Plaintext connection
		client, err = nats.Connect(args.Dsn, creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
	}

	return &NatsJetstream{
		connOpts: connOpts,
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (n *NatsJetstream) Name() string {
	return BackendName
}

func (n *NatsJetstream) Close(_ context.Context) error {
	n.client.Close()
	return nil
}

func (n *NatsJetstream) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetNatsJetstream()
	if args == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

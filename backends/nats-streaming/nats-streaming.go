package nats_streaming

import (
	"context"
	"net/url"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "nats-streaming"

var (
	ErrMissingTLSKey     = errors.New("--tls-client-key-file cannot be blank if using ssl")
	ErrMissingTlsCert    = errors.New("--tls-client-cert-file cannot be blank if using ssl")
	ErrMissingTLSCA      = errors.New("--tls-ca-file cannot be blank if using ssl")
	ErrEmptyChannel      = errors.New("channel cannot be empty")
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

	var natsClient *nats.Conn
	if uri.Scheme == "tls" || args.TlsOptions.UseTls {
		// TLS Secured connection
		tlsConfig, err := util.GenerateTLSConfig(
			args.TlsOptions.TlsCaCert,
			args.TlsOptions.TlsClientCert,
			args.TlsOptions.TlsClientKey,
			args.TlsOptions.TlsSkipVerify,
		)
		if err != nil {
			return nil, errors.Wrap(err, "Unable to generate TLS Config")
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

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetNatsStreaming()
	if args == nil {
		return validate.ErrMissingConnArgs
	}

	if strings.HasPrefix(args.Dsn, "tls") {
		if len(args.TlsOptions.TlsClientKey) == 0 {
			return ErrMissingTLSKey
		}

		if len(args.TlsOptions.TlsClientCert) == 0 {
			return ErrMissingTlsCert
		}

		if len(args.TlsOptions.TlsCaCert) == 0 {
			return ErrMissingTLSCA
		}
	}

	return nil
}

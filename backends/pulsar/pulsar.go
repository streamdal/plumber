package pulsar

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

const BackendName = "pulsar"

var (
	ErrEmptyTopic            = errors.New("topic cannot be empty")
	ErrEmptySubscriptionName = errors.New("subscription name cannot be empty")
	ErrAuthConflict          = errors.New("multiple authentication methods cannot be provided")
)

type Pulsar struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.PulsarConn
	client   pulsar.Client
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*Pulsar, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	client, err := pulsar.NewClient(*getClientOptions(connOpts))
	if err != nil {
		return nil, errors.Wrap(err, "Could not instantiate Pulsar client")
	}

	return &Pulsar{
		connOpts: connOpts,
		connArgs: connOpts.GetPulsar(),
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil

}

func getClientOptions(connOpts *opts.ConnectionOptions) *pulsar.ClientOptions {
	args := connOpts.GetPulsar()

	clientOpts := &pulsar.ClientOptions{
		URL:                        args.Dsn,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          util.DurationSec(args.ConnectTimeoutSeconds),
		TLSAllowInsecureConnection: args.TlsSkipVerify,
	}

	if len(args.ListenerName) > 0 {
		clientOpts.ListenerName = args.ListenerName
	}

	if len(args.TlsClientCert) > 0 && len(args.TlsClientKey) > 0 {
		if util.FileExists(args.TlsClientCert) {
			// Certs inputted as files
			clientOpts.Authentication = pulsar.NewAuthenticationTLS(
				string(args.TlsClientCert),
				string(args.TlsClientKey),
			)
		} else {
			// Certs inputted as strings
			clientOpts.Authentication = pulsar.NewAuthenticationFromTLSCertSupplier(func() (*tls.Certificate, error) {
				return &tls.Certificate{
					Certificate: [][]byte{[]byte(args.TlsClientCert)},
					PrivateKey:  args.TlsClientKey,
				}, nil
			})
		}
	}

	if len(args.Token) > 0 {
		if util.FileExists(args.Token) {
			clientOpts.Authentication = pulsar.NewAuthenticationTokenFromFile(args.Token)
		} else {
			clientOpts.Authentication = pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
				return args.Token, nil
			})
		}
	}

	return clientOpts
}

func (p *Pulsar) Name() string {
	return BackendName
}

func (p *Pulsar) Close(_ context.Context) error {
	p.client.Close() // no return value
	return nil
}

func (p *Pulsar) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	pulsarOpts := connOpts.GetPulsar()
	if pulsarOpts == nil {
		return validate.ErrMissingConnArgs
	}

	if pulsarOpts.Dsn == "" {
		return validate.ErrMissingDSN
	}

	if pulsarOpts.ConnectTimeoutSeconds <= 0 {
		return validate.ErrInvalidConnTimeout
	}

	if len(pulsarOpts.TlsClientCert) > 0 && len(pulsarOpts.TlsClientKey) == 0 {
		return validate.ErrMissingClientKey
	}

	if len(pulsarOpts.TlsClientKey) > 0 && len(pulsarOpts.TlsClientCert) == 0 {
		return validate.ErrMissingClientCert
	}

	if len(pulsarOpts.Token) > 0 && (len(pulsarOpts.TlsClientCert) > 0 || len(pulsarOpts.TlsClientKey) > 0) {
		return ErrAuthConflict
	}

	return nil
}

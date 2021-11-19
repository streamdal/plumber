package pulsar

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "pulsar"

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

	clientOpts := pulsar.ClientOptions{
		URL:                        connOpts.GetPulsar().Dsn,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          util.DurationSec(connOpts.GetPulsar().ConnectTimeoutSeconds),
		TLSAllowInsecureConnection: connOpts.GetPulsar().InsecureTls,
	}

	if len(connOpts.GetPulsar().TlsClientCert) > 0 && len(connOpts.GetPulsar().TlsClientKey) > 0 {
		if fileutil.Exist(string(connOpts.GetPulsar().TlsClientCert)) {
			// Certs inputted as files
			clientOpts.Authentication = pulsar.NewAuthenticationTLS(
				string(connOpts.GetPulsar().TlsClientCert),
				string(connOpts.GetPulsar().TlsClientKey),
			)
		} else {
			// Certs inputted as strings
			clientOpts.Authentication = pulsar.NewAuthenticationFromTLSCertSupplier(func() (*tls.Certificate, error) {
				return &tls.Certificate{
					Certificate: [][]byte{connOpts.GetPulsar().TlsClientCert},
					PrivateKey:  connOpts.GetPulsar().TlsClientKey,
				}, nil
			})
		}
	}

	client, err := pulsar.NewClient(clientOpts)
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

	if connOpts.GetPulsar() == nil {
		return validate.ErrMissingConnArgs
	}

	if len(connOpts.GetPulsar().TlsClientCert) > 0 && len(connOpts.GetPulsar().TlsClientKey) == 0 {
		return validate.ErrMissingClientKey
	}

	if len(connOpts.GetPulsar().TlsClientKey) > 0 && len(connOpts.GetPulsar().TlsClientCert) == 0 {
		return validate.ErrMissingClientCert
	}

	return nil
}

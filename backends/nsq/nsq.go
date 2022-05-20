package nsq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "NSQ"

var (
	ErrMissingAddress = errors.New("you must specify either --nsqd-address or --lookupd-address")
	ErrChooseAddress  = errors.New("you must specify either --nsqd-address or --lookupd-address, not both")
	ErrMissingTopic   = errors.New("--topic cannot be empty")
)

type NSQ struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.NSQConn

	config *nsq.Config

	log *NSQLogger
}

func New(opts *opts.ConnectionOptions) (*NSQ, error) {
	if err := validateBaseConnOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	connArgs := opts.GetNsq()

	logger := &NSQLogger{}
	logger.Entry = logrus.WithField("pkg", "nsq")

	config, err := getNSQConfig(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create NSQ config")
	}

	r := &NSQ{
		connOpts: opts,
		connArgs: connArgs,
		config:   config,
		log:      logger,
	}

	return r, nil
}

// getNSQConfig returns the config needed for creating a new NSQ consumer or producer
func getNSQConfig(opts *opts.ConnectionOptions) (*nsq.Config, error) {
	nsqOpts := opts.GetNsq()

	config := nsq.NewConfig()
	config.ClientID = nsqOpts.ClientId

	if nsqOpts.AuthSecret != "" {
		config.AuthSecret = nsqOpts.AuthSecret
	}

	if nsqOpts.UseTls || len(nsqOpts.TlsClientCert) > 0 {
		tlsConfig, err := generateTLSConfig(opts)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate TLS config")
		}

		config.TlsConfig = tlsConfig
	}

	return config, nil
}

// generateTLSConfig generates necessary TLS config for Dialing to an NSQ server
func generateTLSConfig(opts *opts.ConnectionOptions) (*tls.Config, error) {
	nsqOpts := opts.GetNsq()

	// No client certs
	if len(nsqOpts.TlsClientCert) == 0 {
		return &tls.Config{
			InsecureSkipVerify: nsqOpts.TlsSkipVerify,
		}, nil
	}

	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(string(nsqOpts.TlsCaCert))
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(string(nsqOpts.TlsClientCert), string(nsqOpts.TlsClientKey))
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
		InsecureSkipVerify: nsqOpts.TlsSkipVerify,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

// NSQLogger wraps logrus and implements the Output() method so we can satisfy the interface
// requirements for NSQ's logger
type NSQLogger struct {
	*logrus.Entry
}

// Output writes an NSQ log message via logrus
func (n *NSQLogger) Output(_ int, s string) error {
	n.Info(s)
	return nil
}

func (n *NSQ) Name() string {
	return BackendName
}

func (n *NSQ) Close(_ context.Context) error {
	return nil
}

func (n *NSQ) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	nsqOpts := connOpts.GetNsq()
	if nsqOpts == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

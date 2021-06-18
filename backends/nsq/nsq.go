package nsq

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/jhump/protoreflect/desc"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

var (
	ErrMissingAddress = errors.New("you must specify either --nsqd-address or --lookupd-address")
	ErrChooseAddress  = errors.New("you must specify either --nsqd-address or --lookupd-address, not both")
	ErrMissingTLSKey  = errors.New("--tls-client-key-file cannot be blank if using TLS")
	ErrMissingTlsCert = errors.New("--tls-client-cert-file cannot be blank if using TLS")
	ErrMissingTLSCA   = errors.New("--tls-ca-file cannot be blank if using TLS")
)

// NSQ encapsulates options for calling Read() and Write() methods
type NSQ struct {
	Options *cli.Options
	MsgDesc *desc.MessageDescriptor
	log     *NSQLogger
}

// getNSQConfig returns the config needed for creating a new NSQ consumer or producer
func getNSQConfig(opts *cli.Options) (*nsq.Config, error) {
	config := nsq.NewConfig()
	config.ClientID = opts.NSQ.ClientID

	if opts.NSQ.AuthSecret != "" {
		config.AuthSecret = opts.NSQ.AuthSecret
	}

	if opts.NSQ.InsecureTLS || opts.NSQ.TLSClientCertFile != "" {
		tlsConfig, err := generateTLSConfig(opts)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate TLS config")
		}

		config.TlsConfig = tlsConfig
	}

	return config, nil
}

func generateTLSConfig(opts *cli.Options) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(opts.NSQ.TLSCAFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(opts.NSQ.TLSClientCertFile, opts.NSQ.TLSClientKeyFile)
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
		InsecureSkipVerify: opts.NSQ.InsecureTLS,
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

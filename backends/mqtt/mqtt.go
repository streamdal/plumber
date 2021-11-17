package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

const (
	DefaultClientId = "plumber"
	BackendName     = "mqtt"
)

var (
	ErrInvalidAddress = errors.New("URI scheme must be ssl:// or tcp://")
	ErrMissingTLSKey  = errors.New("--tls-client-key-file cannot be blank if using ssl")
	ErrMissingTlsCert = errors.New("--tls-client-cert-file cannot be blank if using ssl")
	ErrMissingTLSCA   = errors.New("--tls-ca-file cannot be blank if using ssl")
)

type MQTT struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.MQTTConn

	client pahomqtt.Client
	log    *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*MQTT, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	uri, err := url.Parse(connOpts.GetMqtt().Address)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	clientOpts, err := createClientOptions(connOpts.GetMqtt(), uri)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client options")
	}

	client := pahomqtt.NewClient(clientOpts)

	token := client.Connect()

	if !token.WaitTimeout(util.DurationSec(connOpts.GetMqtt().ConnTimeoutSeconds)) {
		return nil, fmt.Errorf("connection timed out after '%d' seconds", connOpts.GetMqtt().ConnTimeoutSeconds)
	}

	if err := token.Error(); err != nil {
		return nil, errors.Wrap(err, "error establishing connection with MQTT broker")
	}

	return &MQTT{
		client:   client,
		connOpts: connOpts,
		connArgs: connOpts.GetMqtt(),
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (m *MQTT) Name() string {
	return BackendName
}

func (m *MQTT) Close(_ context.Context) error {
	m.client.Disconnect(0)
	return nil
}

func (m *MQTT) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func createClientOptions(args *args.MQTTConn, uri *url.URL) (*pahomqtt.ClientOptions, error) {
	opts := pahomqtt.NewClientOptions()

	if uri.Scheme != "ssl" && uri.Scheme != "tcp" {
		return nil, ErrInvalidAddress
	}

	if uri.Scheme == "ssl" {
		tlsConfig, err := generateTLSConfig(args)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate TLS config")
		}

		opts.SetTLSConfig(tlsConfig)
	}

	opts.AddBroker(fmt.Sprintf("%s://%s", uri.Scheme, uri.Host))

	username := uri.User.Username()

	if username != "" {
		opts.SetUsername(username)
		password, _ := uri.User.Password()
		opts.SetPassword(password)
	}

	if args.ClientId == DefaultClientId {
		args.ClientId = fmt.Sprintf("%s-%s", DefaultClientId, uuid.New().String()[0:3])
	}

	opts.SetClientID(args.ClientId)

	return opts, nil
}

func generateTLSConfig(args *args.MQTTConn) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(args.TlsOptions.CaFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(args.TlsOptions.CertFile, args.TlsOptions.KeyFile)
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
		InsecureSkipVerify: args.TlsOptions.SkipVerify,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	mqttOpts := connOpts.GetMqtt()
	if mqttOpts == nil {
		return validate.ErrMissingConnArgs
	}

	if strings.HasPrefix(mqttOpts.Address, "ssl") {
		if mqttOpts.TlsOptions.KeyFile == "" {
			return ErrMissingTLSKey
		}

		if mqttOpts.TlsOptions.CertFile == "" {
			return ErrMissingTlsCert
		}

		if mqttOpts.TlsOptions.CaFile == "" {
			return ErrMissingTLSCA
		}
	}

	return nil
}

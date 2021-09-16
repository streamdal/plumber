package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/batchcorp/plumber/types"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

const (
	BackendName = "mqtt"
)

var (
	errInvalidAddress  = errors.New("URI scheme must be ssl:// or tcp://")
	errMissingAddress  = errors.New("--address cannot be empty")
	errMissingTopic    = errors.New("--topic cannot be empty")
	errMissingTLSKey   = errors.New("--tls-client-key-file cannot be blank if using ssl")
	errMissingTlsCert  = errors.New("--tls-client-cert-file cannot be blank if using ssl")
	errMissingTLSCA    = errors.New("--tls-ca-file cannot be blank if using ssl")
	errInvalidQOSLevel = errors.New("QoS level can only be 0, 1 or 2")
)

type MQTT struct {
	Options *options.Options

	client pahomqtt.Client
	log    *logrus.Entry
}

func New(opts *options.Options) (*MQTT, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := connect(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to MQTT")
	}

	return &MQTT{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "mqtt"),
	}, nil
}

func (m *MQTT) Name() string {
	return BackendName
}

func (m *MQTT) Close(ctx context.Context) error {
	if m.client == nil {
		return nil
	}

	m.client.Disconnect(10000)

	m.client = nil

	return nil
}

func (m *MQTT) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func connect(opts *options.Options) (pahomqtt.Client, error) {
	uri, err := url.Parse(opts.MQTT.Address)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	clientOpts, err := createClientOptions(opts, uri)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client options")
	}

	client := pahomqtt.NewClient(clientOpts)

	token := client.Connect()

	if !token.WaitTimeout(opts.MQTT.Timeout) {
		return nil, fmt.Errorf("connection timed out after %s", opts.MQTT.Timeout)
	}

	if err := token.Error(); err != nil {
		return nil, errors.Wrap(err, "error establishing connection with MQTT broker")
	}

	return client, nil
}

func createClientOptions(cliOpts *options.Options, uri *url.URL) (*pahomqtt.ClientOptions, error) {
	opts := pahomqtt.NewClientOptions()

	if uri.Scheme != "ssl" && uri.Scheme != "tcp" {
		return nil, errInvalidAddress
	}

	if uri.Scheme == "ssl" {
		tlsConfig, err := generateTLSConfig(cliOpts)
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

	opts.SetClientID(cliOpts.MQTT.ClientID)

	return opts, nil
}

func generateTLSConfig(opts *options.Options) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	pemCerts, err := ioutil.ReadFile(opts.MQTT.TLSCAFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(opts.MQTT.TLSClientCertFile, opts.MQTT.TLSClientKeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load ssl keypair")
	}

	// Just to print out the client certificate..
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse certificate")
	}

	// Create tls.ReadOptions with desired tls properties
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: opts.MQTT.InsecureTLS,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.MQTT == nil {
		return errors.New("MQTT options cannot be nil")
	}

	return nil
}

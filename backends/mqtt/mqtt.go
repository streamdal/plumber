package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"
)

const (
	DefaultClientId = "plumber"
	BackendName     = "mqtt"
)

var (
	ErrInvalidAddress = errors.New("URI scheme must be ssl:// or tcp://")
	ErrWriteTimeout   = errors.New("write timeout must be greater than 0")
	ErrEmptyTopic     = errors.New("Topic cannot be empty")
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
		tlsConfig, err := util.GenerateTLSConfig(
			args.TlsOptions.TlsCaCert,
			args.TlsOptions.TlsClientCert,
			args.TlsOptions.TlsClientKey,
			args.TlsOptions.TlsSkipVerify,
			tls.NoClientCert, // TODO: MQTT supports TLS auth and so should we eventually
		)
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

	return nil
}

package mqtt

import (
	"fmt"
	"net/url"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

type MQTT struct {
	Options *cli.Options
	Client  pahomqtt.Client
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

func connect(opts *cli.Options) (pahomqtt.Client, error) {
	uri, err := url.Parse(opts.MQTT.Address)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	clientOpts, err := createClientOptions(opts.MQTT.ClientId, uri)
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

func createClientOptions(clientId string, uri *url.URL) (*pahomqtt.ClientOptions, error) {
	opts := pahomqtt.NewClientOptions()

	if uri.Scheme == "" {
		return nil, errors.New("URI scheme in address cannot be empty (ie. must begin with ssl:// or tcp://)")
	}

	opts.AddBroker(fmt.Sprintf("%s://%s", uri.Scheme, uri.Host))

	username := uri.User.Username()

	if username != "" {
		opts.SetUsername(username)
		password, _ := uri.User.Password()
		opts.SetPassword(password)
	}

	opts.SetClientID(clientId)

	return opts, nil
}

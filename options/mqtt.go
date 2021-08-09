package options

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	MQTTDefaultConnectTimeout = "5s"
	MQTTDefaultClientId       = "plumber"
)

type MQTTOptions struct {
	// Shared
	Address  string
	Topic    string
	Timeout  time.Duration
	ClientID string
	QoSLevel int

	// TLS-related pieces
	TLSCAFile         string
	TLSClientCertFile string
	TLSClientKeyFile  string
	InsecureTLS       bool

	// Read
	ReadTimeout time.Duration

	// Write
	WriteTimeout time.Duration
}

func HandleMQTTFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("mqtt", "MQTT message system")

	addSharedMQTTFlags(rc, opts)
	addReadMQTTFlags(rc, opts)

	wc := writeCmd.Command("mqtt", "MQTT message system")

	addSharedMQTTFlags(wc, opts)
	addWriteMQTTFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("mqtt", "MQTT message system")
	}

	addSharedMQTTFlags(rec, opts)
	addReadMQTTFlags(rec, opts)
}

func addSharedMQTTFlags(cmd *kingpin.CmdClause, opts *Options) {
	clientId := fmt.Sprintf("%s-%s", MQTTDefaultClientId, uuid.New().String()[0:3])

	cmd.Flag("address", "Destination host address").
		Envar("PLUMBER_RELAY_MQTT_ADDRESS").
		Default("tcp://localhost:1883").
		StringVar(&opts.MQTT.Address)

	cmd.Flag("topic", "Topic to read message(s) from").
		Envar("PLUMBER_RELAY_MQTT_TOPIC").
		Required().
		StringVar(&opts.MQTT.Topic)

	cmd.Flag("timeout", "Connect timeout").
		Envar("PLUMBER_RELAY_MQTT_CONNECT_TIMEOUT").
		Default(MQTTDefaultConnectTimeout).
		DurationVar(&opts.MQTT.Timeout)

	cmd.Flag("client-id", "Client id presented to MQTT broker").
		Envar("PLUMBER_RELAY_MQTT_CLIENT_ID").
		Default(clientId).
		StringVar(&opts.MQTT.ClientID)

	cmd.Flag("qos", "QoS level to use for pub/sub (0, 1, 2)").
		Envar("PLUMBER_RELAY_MQTT_QOS").
		Default("0").
		IntVar(&opts.MQTT.QoSLevel)

	cmd.Flag("tls-ca-file", "CA file (only needed if addr is ssl://").
		Envar("PLUMBER_RELAY_MQTT_TLS_CA_FILE").
		ExistingFileVar(&opts.MQTT.TLSCAFile)

	cmd.Flag("tls-client-cert-file", "Client cert file (only needed if addr is ssl://").
		Envar("PLUMBER_RELAY_MQTT_TLS_CERT_FILE").
		ExistingFileVar(&opts.MQTT.TLSClientCertFile)

	cmd.Flag("tls-client-key-file", "Client key file (only needed if addr is ssl://").
		Envar("PLUMBER_RELAY_MQTT_TLS_KEY_FILE").
		ExistingFileVar(&opts.MQTT.TLSClientKeyFile)

	cmd.Flag("insecure-tls", "Whether to verify server certificate").
		Envar("PLUMBER_RELAY_MQTT_SKIP_VERIFY_TLS").
		Default("false").
		BoolVar(&opts.MQTT.InsecureTLS)
}

func addReadMQTTFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("read-timeout", "How long to wait for a message (default: forever)").
		Default("0s").DurationVar(&opts.MQTT.ReadTimeout)
}

func addWriteMQTTFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("write-timeout", "How long to attempt to publish a message").
		Default("5s").DurationVar(&opts.MQTT.WriteTimeout)
}

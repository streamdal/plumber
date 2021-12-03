// Package kafka is the most complex backend as it has several different
// operating modes. Due to this, it should not be used as a "template" for other
// backends - most other backends are significantly simpler and follow an
// expected flow.
package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

const (
	BackendName = "kafka"

	DefaultBatchSize = 1
)

var (
	ErrMissingTopic = errors.New("You must specify at least one topic")
)

// Kafka holds all attributes required for performing a write to Kafka. This
// struct should be instantiated via the kafka.Read(..) func.
type Kafka struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.KafkaConn

	dialer *skafka.Dialer
	log    *logrus.Entry
}

type Reader struct {
	Conn   *skafka.Conn
	Reader *skafka.Reader
}

type Writer struct {
	Writer *skafka.Writer
}

func New(connOpts *opts.ConnectionOptions) (*Kafka, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	dialer, err := newDialer(connOpts.GetKafka())
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new dialer")
	}

	return &Kafka{
		connOpts: connOpts,
		connArgs: connOpts.GetKafka(),
		dialer:   dialer,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (k *Kafka) Name() string {
	return BackendName
}

// Close is a noop for kafka because read/write/lag/etc. all handle conn setup
// and teardown on their own.
func (k *Kafka) Close(_ context.Context) error {
	return nil
}

func (k *Kafka) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func NewReaderForRead(dialer *skafka.Dialer, connArgs *args.KafkaConn, readArgs *args.KafkaReadArgs) (*skafka.Reader, error) {
	rc := skafka.ReaderConfig{
		Brokers:          connArgs.Address,
		CommitInterval:   util.DurationSec(readArgs.CommitIntervalSeconds),
		Dialer:           dialer,
		MaxWait:          util.DurationSec(readArgs.MaxWaitSeconds),
		MinBytes:         int(readArgs.MinBytes),
		MaxBytes:         int(readArgs.MaxBytes),
		QueueCapacity:    int(readArgs.QueueCapacity),
		RebalanceTimeout: util.DurationSec(readArgs.RebalanceTimeoutSeconds),
	}

	if readArgs.UseConsumerGroup {
		rc.GroupTopics = readArgs.Topics
		rc.GroupID = readArgs.ConsumerGroupName
	} else {
		rc.Topic = readArgs.Topics[0]
	}

	r := skafka.NewReader(rc)

	if !readArgs.UseConsumerGroup {
		if err := r.SetOffset(readArgs.ReadOffset); err != nil {
			return nil, errors.Wrap(err, "unable to set read offset")
		}
	}

	return r, nil
}

func NewReaderForRelay(dialer *skafka.Dialer, connArgs *args.KafkaConn, relayArgs *args.KafkaRelayArgs) (*skafka.Reader, error) {
	rc := skafka.ReaderConfig{
		Brokers:          connArgs.Address,
		CommitInterval:   util.DurationSec(relayArgs.CommitIntervalSeconds),
		Dialer:           dialer,
		MaxWait:          util.DurationSec(relayArgs.MaxWaitSeconds),
		MinBytes:         int(relayArgs.MinBytes),
		MaxBytes:         int(relayArgs.MaxBytes),
		QueueCapacity:    int(relayArgs.QueueCapacity),
		RebalanceTimeout: util.DurationSec(relayArgs.RebalanceTimeoutSeconds),
	}

	if relayArgs.UseConsumerGroup {
		rc.GroupTopics = relayArgs.Topics
		rc.GroupID = relayArgs.ConsumerGroupName
	} else {
		rc.Topic = relayArgs.Topics[0]
	}

	r := skafka.NewReader(rc)

	if !relayArgs.UseConsumerGroup {
		if err := r.SetOffset(relayArgs.ReadOffset); err != nil {
			return nil, errors.Wrap(err, "unable to set read offset")
		}
	}

	return r, nil
}

// NewWriter creates a new instance of a writer that can write messages to a topic.
// NOTE: Continuing to use the deprecated NewWriter() func to avoid dealing with
// TLS issues (since *Writer does not have a Dialer and Transport has TLS
// defined separate from the dialer).
func NewWriter(dialer *skafka.Dialer, connArgs *args.KafkaConn, topics ...string) (*skafka.Writer, error) {
	// Necessary for auto-creating topics on writes, if enabled on the server
	for _, t := range topics {
		conn, err := connect(dialer, connArgs, t, 0)
		if err != nil {
			return nil, fmt.Errorf("unable to establish connection to leader for topic '%s': %s", t, err)
		}
		conn.Close()
	}

	// NOTE: We explicitly do NOT set the topic - it will be set in the message
	return skafka.NewWriter(skafka.WriterConfig{
		Brokers:   connArgs.Address,
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	}), nil
}

func ConnectAllTopics(dialer *skafka.Dialer, connArgs *args.KafkaConn, topics []string) (map[string]*skafka.Conn, error) {
	conns := make(map[string]*skafka.Conn, 0)

	for _, topicName := range topics {
		conn, err := connect(dialer, connArgs, topicName, 0)
		if err != nil {
			return nil, fmt.Errorf("unable to create conn for topic '%s': %s", topicName, err)
		}

		conns[topicName] = conn
	}

	return conns, nil
}

// Generic connect that will dial the leader for a topic and partition.
func connect(dialer *skafka.Dialer, connArgs *args.KafkaConn, topic string, partition int) (*skafka.Conn, error) {
	for _, brokerAddress := range connArgs.Address {
		// The dialer timeout does not get utilized under some conditions (such as
		// when kafka is configured to NOT auto create topics) - we need a
		// mechanism to bail out early.
		connTimeout := time.Duration(connArgs.TimeoutSeconds) * time.Second

		ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(connTimeout))

		conn, err := dialer.DialLeader(ctxDeadline, "tcp", brokerAddress, topic, partition)
		if err != nil {
			logrus.Errorf("unable to create leader connection to broker '%s' for topicName '%s', "+
				"trying next broker", brokerAddress, topic)
			continue
		}

		logrus.Debugf("found leader for topic '%s' via broker '%s'", topic, brokerAddress)

		return conn, nil
	}

	return nil, errors.New("unable to connect to any brokers")
}

// getAuthenticationMechanism returns the correct authentication config for use with kafka.Dialer if a username/password
// is provided. If not, it will return nil
func getAuthenticationMechanism(connArgs *args.KafkaConn) (sasl.Mechanism, error) {
	if connArgs.SaslUsername == "" {
		return nil, nil
	}

	// Username given, but no password. Prompt user for it
	if connArgs.SaslPassword == "" {
		password, err := readPassword()
		if err != nil {
			return nil, errors.Wrap(err, "unable to read password from STDIN")
		}
		connArgs.SaslPassword = password
	}

	switch strings.ToLower(connArgs.SaslType.String()) {
	case "scram":
		return scram.Mechanism(scram.SHA512, connArgs.SaslUsername, connArgs.SaslPassword)
	default:
		return plain.Mechanism{
			Username: connArgs.SaslUsername,
			Password: connArgs.SaslPassword,
		}, nil
	}
}

// readPassword prompts the user for a password from stdin
func readPassword() (string, error) {
	for {
		fmt.Print("Enter Password: ")

		// int typecast is needed for windows
		password, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", errors.New("you must enter a password")
		}

		fmt.Println("")

		sp := strings.TrimSpace(string(password))
		if sp != "" {
			return sp, nil
		}
	}
}

// convertKafkaHeadersToProto converts type of header slice from segmentio's to our protobuf type
func convertKafkaHeadersToProto(original []skafka.Header) []*records.KafkaHeader {
	converted := make([]*records.KafkaHeader, 0)

	for _, o := range original {
		converted = append(converted, &records.KafkaHeader{
			Key:   o.Key,
			Value: string(o.Value),
		})
	}

	return converted
}

func newDialer(connArgs *args.KafkaConn) (*skafka.Dialer, error) {
	dialer := &skafka.Dialer{
		Timeout: time.Duration(connArgs.TimeoutSeconds) * time.Second,
	}

	if connArgs.TlsSkipVerify {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(connArgs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get auth mechanism")
	}

	dialer.SASLMechanism = auth

	return dialer, nil
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	if connOpts.GetKafka() == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

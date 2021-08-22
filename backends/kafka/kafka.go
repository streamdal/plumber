package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/reader"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

const (
	BackendName = "kafka"

	DefaultBatchSize = 1
)

// Kafka holds all attributes required for performing a write to Kafka. This
// struct should be instantiated via the kafka.Read(..) func.
type Kafka struct {
	Options *options.Options

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

func New(opts *options.Options) (*Kafka, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	dialer, err := newDialer(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new dialer")
	}

	return &Kafka{
		Options: opts,
		dialer:  dialer,
		log:     logrus.WithField("backend", "kafka"),
	}, nil
}

func (k *Kafka) Name() string {
	return BackendName
}

// Close is a noop for kafka because read/write/lag/etc. all handle conn setup
// and teardown on their own.
func (k *Kafka) Close(ctx context.Context) error {
	return nil
}

func (k *Kafka) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

// TODO: If read message contains record - there's no need for this func; each
// backend would fill out the necessary record bits
func (k *Kafka) ConvertReadToRecord(msgID, plumberID string, readMsg *types.ReadMessage) (*records.Message, error) {
	if readMsg == nil {
		return nil, errors.New("read message cannot be nil")
	}

	rawMsg, ok := readMsg.Raw.(skafka.Message)
	if !ok {
		return nil, errors.New("unable to assert raw message")
	}

	// Try to decode the value
	// TODO: Make sure that opts.MessageDesc is set so Decode can work
	decoded, err := reader.Decode(k.Options, rawMsg.Value)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode value")
	}

	return &records.Message{
		MessageId:        msgID,
		PlumberId:        plumberID,
		UnixTimestampUtc: readMsg.ReceivedAt.UnixNano(),
		Decoded:          decoded,
		Message: &records.Message_Kafka{
			Kafka: &records.Kafka{
				Topic:     rawMsg.Topic,
				Key:       rawMsg.Key,
				Value:     rawMsg.Value, // original payload
				Timestamp: rawMsg.Time.UnixNano(),
				Offset:    rawMsg.Offset,
				Partition: int32(rawMsg.Partition),
				Headers:   convertKafkaHeadersToProto(rawMsg.Headers),
			},
		},
	}, nil
}

func NewReader(dialer *skafka.Dialer, opts *options.Options) (*skafka.Reader, error) {
	rc := skafka.ReaderConfig{
		Brokers:          opts.Kafka.Brokers,
		CommitInterval:   opts.Kafka.CommitInterval,
		Dialer:           dialer,
		MaxWait:          opts.Kafka.MaxWait,
		MinBytes:         opts.Kafka.MinBytes,
		MaxBytes:         opts.Kafka.MaxBytes,
		QueueCapacity:    opts.Kafka.QueueCapacity,
		RebalanceTimeout: opts.Kafka.RebalanceTimeout,
	}

	if opts.Kafka.UseConsumerGroup {
		rc.GroupTopics = opts.Kafka.Topics
		rc.GroupID = opts.Kafka.GroupID
	} else {
		rc.Topic = opts.Kafka.Topics[0]
	}

	r := skafka.NewReader(rc)

	if !opts.Kafka.UseConsumerGroup {
		if err := r.SetOffset(opts.Kafka.ReadOffset); err != nil {
			return nil, errors.Wrap(err, "unable to set read offset")
		}
	}

	return r, nil
}

// NewWriter creates a new instance of a writer that can write messages to a topic.
// NOTE: Continuing to use the deprecated NewWriter() func to avoid dealing with
// TLS issues (since *Writer does not have a Dialer and Transport has TLS
// defined separate from the dialer).
func NewWriter(dialer *skafka.Dialer, opts *options.Options) (*skafka.Writer, error) {
	// NOTE: We explicitly do NOT set the topic - it will be set in the message
	return skafka.NewWriter(skafka.WriterConfig{
		Brokers:   opts.Kafka.Brokers,
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	}), nil
}

func ConnectAllTopics(dialer *skafka.Dialer, opts *options.Options) (map[string]*skafka.Conn, error) {
	conns := make(map[string]*skafka.Conn, 0)

	for _, topicName := range opts.Kafka.Topics {
		conn, err := connect(dialer, opts, topicName, 0)
		if err != nil {
			return nil, fmt.Errorf("unable to create conn for topic '%s': %s", topicName, err)
		}

		conns[topicName] = conn
	}

	return conns, nil
}

// Generic connect that will dial the leader for a topic and partition.
func connect(dialer *skafka.Dialer, opts *options.Options, topic string, partition int) (*skafka.Conn, error) {
	for _, brokerAddress := range opts.Kafka.Brokers {
		// The dialer timeout does not get utilized under some conditions (such as
		// when kafka is configured to NOT auto create topics) - we need a
		// mechanism to bail out early.
		ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

		conn, err := dialer.DialLeader(ctxDeadline, "tcp", brokerAddress, topic, partition)
		if err != nil {
			logrus.Errorf("unable to create leader connection to broker '%s' for topicName '%s', "+
				"trying next broker", topic, brokerAddress)
			continue
		}

		logrus.Debugf("found leader for topic '%s' via broker '%s'", topic, brokerAddress)

		return conn, nil
	}

	return nil, errors.New("unable to connect to any brokers")
}

// getAuthenticationMechanism returns the correct authentication config for use with kafka.Dialer if a username/password
// is provided. If not, it will return nil
func getAuthenticationMechanism(opts *options.Options) (sasl.Mechanism, error) {
	if opts.Kafka.Username == "" {
		return nil, nil
	}

	// Username given, but no password. Prompt user for it
	if opts.Kafka.Password == "" {
		password, err := readPassword()
		if err != nil {
			return nil, errors.Wrap(err, "unable to read password from STDIN")
		}
		opts.Kafka.Password = password
	}

	switch strings.ToLower(opts.Kafka.AuthenticationType) {
	case "scram":
		return scram.Mechanism(scram.SHA512, opts.Kafka.Username, opts.Kafka.Password)
	default:
		return plain.Mechanism{
			Username: opts.Kafka.Username,
			Password: opts.Kafka.Password,
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

func newDialer(opts *options.Options) (*skafka.Dialer, error) {
	dialer := &skafka.Dialer{
		Timeout: opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get auth mechanism")
	}

	dialer.SASLMechanism = auth

	return dialer, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.Kafka == nil {
		return errors.New("kafka options cannot be nil")
	}

	return nil
}

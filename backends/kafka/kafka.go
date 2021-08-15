package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber/options"
)

const (
	DefaultBatchSize = 1
)

// Kafka holds all attributes required for performing a write to Kafka. This
// struct should be instantiated via the kafka.Read(..) func.
type Kafka struct {
	Options *options.Options

	id     string
	conns  map[string]*skafka.Conn
	reader *skafka.Reader
	writer *skafka.Writer
	log    *logrus.Entry
}

type Reader struct {
	Reader *skafka.Reader
}

type Writer struct {
	Writer *skafka.Writer
}

type Lagger struct {
	partitionDiscoverConn map[string]*skafka.Conn
}

func New(opts *options.Options) (*Kafka, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	dialer, err := newDialer(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new dialer")
	}

	conns, err := connect(dialer, opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new connections")
	}

	return &Kafka{
		Options: opts,
		conns:   conns,
		log:     logrus.WithField("backend", "kafka"),
	}, nil
}

//
//func NewKafkaLagConnection(opts *options.Options) (*Lagger, error) {
//	dialer := &skafka.Dialer{
//		DualStack: true,
//		Timeout:   opts.Kafka.Timeout,
//	}
//
//	if opts.Kafka.InsecureTLS {
//		dialer.TLS = &tls.Config{
//			InsecureSkipVerify: true,
//		}
//	}
//
//	auth, err := getAuthenticationMechanism(opts)
//	if err != nil {
//		return nil, errors.Wrap(err, "unable to get authentication mechanism")
//	}
//
//	dialer.SASLMechanism = auth
//
//	// The dialer timeout does not get utilized under some conditions (such as
//	// when kafka is configured to NOT auto create topics) - we need a
//	// mechanism to bail out early.
//	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))
//
//	connMap := make(map[string]*skafka.Conn, len(opts.Kafka.Topics))
//
//	// Establish connection with leader broker
//
//	for _, v := range opts.Kafka.Topics {
//		conn, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Brokers[0], v, 0)
//		if err != nil {
//			return nil, errors.Wrap(err, "unable to dial leader")
//		}
//
//		connMap[v] = conn
//	}
//
//	kLag := &Lagger{
//		partitionDiscoverConn: connMap,
//	}
//
//	return kLag, err
//
//}

func newConnectionPerPartition(dialer *skafka.Dialer, topic string, partition int, opts *options.Options) (*skafka.Conn, error) {
	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	var conn *skafka.Conn
	var err error

	for _, brokerAddress := range opts.Kafka.Brokers {
		conn, err = dialer.DialLeader(ctxDeadline, "tcp", brokerAddress, topic, partition)
		if err != nil {
			logrus.Warningf("unable to establish leader connection for topic '%s', partition '%d' "+
				"via broker '%s' (trying additional brokers)", topic, partition, brokerAddress)
			continue
		}
	}

	if err != nil {
		return nil, fmt.Errorf("unable to dial leader for topic '%s', partition '%d' - "+
			"exhausted all brokers", topic, partition)
	}

	return conn, nil
}

func NewReader(dialer *skafka.Dialer, opts *options.Options) (*Reader, error) {
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

	return &Reader{
		Reader: r,
	}, nil
}

// TODO: This needs to be updated to NOT use NewWriter since it's deprecated
func NewWriter(opts *options.Options) (*Writer, error) {
	dialer, err := newDialer(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new dialer")
	}

	conns, err := newConn(dialer, opts)
	if err != nil {
		return nil, err
	}

	// NOTE: We explicitly do NOT set the topic - it will be set in the message
	w := skafka.NewWriter(skafka.WriterConfig{
		Brokers:   opts.Kafka.Brokers,
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	})

	return &Writer{
		Writer: w,
		Conns:  conn,
	}, nil
}

// connect attempts to kafka.DialLeader() for _all_ provided topics and  returns
// a map where key is topic and value is connection to the leader.
func connect(dialer *skafka.Dialer, opts *options.Options) (map[string]*skafka.Conn, error) {
	conns := make(map[string]*skafka.Conn, 0)

	for _, topicName := range opts.Kafka.Topics {
		for _, brokerAddress := range opts.Kafka.Brokers {
			// The dialer timeout does not get utilized under some conditions (such as
			// when kafka is configured to NOT auto create topics) - we need a
			// mechanism to bail out early.
			ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

			conn, err := dialer.DialLeader(ctxDeadline, "tcp", brokerAddress, topicName, 0)
			if err != nil {
				logrus.Errorf("unable to create leader connection to broker '%s' for topicName '%s', "+
					"trying next broker", topicName, brokerAddress)
				continue
			}

			logrus.Debugf("found leader for topic '%s' via broker '%s'", topicName, brokerAddress)

			conns[topicName] = conn
		}

		if _, ok := conns[topicName]; !ok {
			return nil, fmt.Errorf("unable to get leader connection for topic '%s' - exhausted all brokers", topicName)
		}
	}

	return conns, nil
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

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.Kafka == nil {
		return errors.New("kafka options cannot be nil")
	}

	return nil
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

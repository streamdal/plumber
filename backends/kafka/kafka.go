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

	id      string
	reader  *skafka.Reader
	writer  *skafka.Writer
	msgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

type Reader struct {
	Reader *skafka.Reader
	Conn   *skafka.Conn
}

type Writer struct {
	Writer *skafka.Writer
	Conn   *skafka.Conn
}

type Lagger struct {
	partitionDiscoverConn map[string]*skafka.Conn
}

func New(opts *options.Options) (*Kafka, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &Kafka{
		Options: opts,
		log:     logrus.WithField("backend", "kafka"),
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

func NewKafkaLagConnection(opts *options.Options) (*Lagger, error) {
	dialer := &skafka.Dialer{
		DualStack: true,
		Timeout:   opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get authentication mechanism")
	}

	dialer.SASLMechanism = auth

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	connMap := make(map[string]*skafka.Conn, len(opts.Kafka.Topics))

	// Establish connection with with leader broker

	for _, v := range opts.Kafka.Topics {

		conn, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Brokers[0], v, 0)

		if err != nil {
			return nil, err
		}

		connMap[v] = conn
	}

	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Brokers[0], err)
	}

	kLag := &Lagger{
		partitionDiscoverConn: connMap,
	}

	return kLag, err

}

func newConnectionPerPartition(topic string, partition int, opts *options.Options) (*skafka.Conn, error) {
	dialer := &skafka.Dialer{
		DualStack: true,
		Timeout:   opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get authentication mechanism")
	}

	dialer.SASLMechanism = auth

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	// Establish connection with with leader broker

	conn, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Brokers[0], topic, partition)

	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Brokers[0], err)
	}

	return conn, err

}

func NewReader(opts *options.Options) (*Reader, error) {
	dialer := &skafka.Dialer{
		DualStack: true,
		Timeout:   opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get authentication mechanism")
	}

	dialer.SASLMechanism = auth

	// Attempt to establish connection on startup
	conn, err := dialContext(dialer, opts)
	if err != nil {
		return nil, err
	}

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
		Conn:   conn,
	}, nil
}

func NewWriter(opts *options.Options) (*Writer, error) {
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
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Brokers[0], err)
	}

	dialer.SASLMechanism = auth

	conn, err := dialLeader(dialer, opts)
	if err != nil {
		return nil, err
	}

	w := skafka.NewWriter(skafka.WriterConfig{
		Brokers:   opts.Kafka.Brokers,
		Topic:     opts.Kafka.Topics[0],
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	})

	return &Writer{
		Writer: w,
		Conn:   conn,
	}, nil
}

// dialContext attempts to kafka.DialLeader() for any of the brokers a user has provided
func dialContext(dialer *skafka.Dialer, opts *options.Options) (*skafka.Conn, error) {
	var err error
	var conn *skafka.Conn

	// Attempt to establish connection on startup
	for _, address := range opts.Kafka.Brokers {
		// The dialer timeout does not get utilized under some conditions (such as
		// when kafka is configured to NOT auto create topics) - we need a
		// mechanism to bail out early.
		ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

		conn, err = dialer.DialContext(ctxDeadline, "tcp", address)
		if err != nil {
			logrus.Errorf("unable to create initial connection to broker '%s', trying next broker", address)
			continue
		}

		logrus.Infof("Connected to kafka broker '%s'", address)
		return conn, nil
	}

	// Did not succeed connecting to any broker
	return nil, errors.Wrap(err, "unable to connect to any broker")
}

// dialLeader attempts to kafka.DialLeader() for any of the brokers a user has provided
func dialLeader(dialer *skafka.Dialer, opts *options.Options) (*skafka.Conn, error) {
	var err error
	var conn *skafka.Conn

	// Attempt to establish connection on startup
	for _, address := range opts.Kafka.Brokers {
		// The dialer timeout does not get utilized under some conditions (such as
		// when kafka is configured to NOT auto create topics) - we need a
		// mechanism to bail out early.
		ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

		conn, err = dialer.DialLeader(ctxDeadline, "tcp", address, opts.Kafka.Topics[0], 0)
		if err != nil {
			logrus.Errorf("unable to create initial connection to broker '%s', trying next broker", address)
			continue
		}

		logrus.Infof("Connected to kafka broker '%s'", address)
		return conn, nil
	}

	// Did not succeed connecting to any broker
	return nil, errors.Wrap(err, "unable to connect to any broker")
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
			return nil, err
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

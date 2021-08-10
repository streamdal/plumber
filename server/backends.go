package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"

	"github.com/batchcorp/plumber/backends/kafka"
)

// TODO: code in this package is duplicated from kafka backend. Clean this up after backend refactor

// testConnection is called by PlumberServer.TestConnection and determines if we are able to
// successfully connect to the target message bus
func testConnection(conn *protos.Connection) error {
	switch {
	case conn.GetKafka() != nil:
		return testConnectionKafka(conn.GetKafka())
	}

	return errors.New("unknown connection type")
}

// testConnectionKafka determines if we can connect to the Kafka broker(s)
func testConnectionKafka(connCfg *conns.Kafka) error {
	dialer, err := getKafkaDialer(connCfg)
	if err != nil {
		return errors.Wrap(err, "unable to connect to Kafka")
	}

	connTimeout, err := time.ParseDuration(fmt.Sprintf("%ds", connCfg.TimeoutSeconds))
	if err != nil {
		return errors.Wrap(err, "unable to parse RebalanceTimeoutSeconds")
	}

	for _, broker := range connCfg.Address {
		ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(connTimeout))

		conn, err := dialer.DialContext(ctxDeadline, "tcp", broker)
		if err != nil {
			return errors.Wrapf(err, "unable to connect to Kafka broker '%s'", broker)
		}
		conn.Close()
	}

	return nil
}

// getBackendRead gets the backend message bus needed to read/write from
func (p *PlumberServer) getBackendRead(read *protos.Read) (*kafka.Reader, error) {
	connCfg := p.getConn(read.ConnectionId)
	if connCfg == nil {
		return nil, errors.New("connection does not exist")
	}

	switch {
	case read.GetKafka() != nil:
		args := read.GetKafka()
		samp := read.GetSampleOptions()
		return getBackendReadKafka(connCfg.Connection.GetKafka(), args, samp)
	}

	return nil, errors.New("unknown message bus")
}

// getKafkaDialer generates a kafka dialer based on the provided config
func getKafkaDialer(kafkaCfg *conns.Kafka) (*skafka.Dialer, error) {
	dialer := &skafka.Dialer{
		DualStack: true,
		Timeout:   time.Second * 10,
	}

	if kafkaCfg.InsecureTls {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getKafkaAuthConfig(kafkaCfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not get authentication mechanism")
	}

	dialer.SASLMechanism = auth

	return dialer, nil
}

// getBackendReadKafka returns a properly configured kafka reader instance
func getBackendReadKafka(connCfg *conns.Kafka, readArgs *args.Kafka, sampleOptions *protos.SampleOptions) (*kafka.Reader, error) {
	dialer, err := getKafkaDialer(connCfg)
	if err != nil {
		return nil, err
	}

	commitInterval, err := time.ParseDuration(fmt.Sprintf("%ds", readArgs.CommitIntervalSeconds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse CommitIntervalSeconds")
	}

	maxWait, err := time.ParseDuration(fmt.Sprintf("%ds", readArgs.MaxWaitSeconds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse MaxWaitSeconds")
	}

	rebalanceTimeout, err := time.ParseDuration(fmt.Sprintf("%ds", readArgs.RebalanceTimeoutSeconds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse RebalanceTimeoutSeconds")
	}

	rc := skafka.ReaderConfig{
		Brokers:          connCfg.Address,
		CommitInterval:   commitInterval,
		Dialer:           dialer,
		MaxWait:          maxWait,
		MinBytes:         int(readArgs.MinBytes),
		MaxBytes:         int(readArgs.MaxBytes),
		QueueCapacity:    100, // TODO: add to protos?
		RebalanceTimeout: rebalanceTimeout,
	}

	if sampleOptions != nil {
		// Sampling mode. No consumer group
		rc.Topic = readArgs.Topics[0]
		rc.StartOffset = readArgs.ReadOffset
	} else {
		// Non sampling mode
		if readArgs.UseConsumerGroup {
			rc.GroupTopics = readArgs.Topics
			rc.GroupID = readArgs.ConsumerGroupName
		} else {
			rc.Topic = readArgs.Topics[0]
			rc.StartOffset = readArgs.ReadOffset
		}
	}

	r := skafka.NewReader(rc)

	return &kafka.Reader{
		Reader: r,
	}, nil
}

// getBackendWrite gets the backend message bus needed to read/write from
// TODO: genericize after backend refactor
func (p *PlumberServer) getBackendWrite(req *protos.WriteRequest) (*kafka.Writer, error) {
	connCfg := p.getConn(req.ConnectionId)
	if connCfg == nil {
		return nil, errors.New("connection does not exist")
	}

	switch {
	case connCfg.GetKafka() != nil:
		return getBackendWriteKafka(connCfg.GetKafka())
	}

	return nil, errors.New("unknown message bus")
}

// getBackendWriteKafka returns a properly configured kafka writer instance
func getBackendWriteKafka(connCfg *conns.Kafka) (*kafka.Writer, error) {
	dialer, err := getKafkaDialer(connCfg)
	if err != nil {
		return nil, err
	}
	writer := skafka.NewWriter(skafka.WriterConfig{
		Brokers:   connCfg.Address,
		Dialer:    dialer,
		BatchSize: 1, // TODO: add to protos?
	})

	return &kafka.Writer{
		Writer: writer,
	}, nil
}

// getKafkaAuthConfig returns authentication mechanism for Kafka, if one is needed, nil otherwise
func getKafkaAuthConfig(connCfg *conns.Kafka) (sasl.Mechanism, error) {
	switch connCfg.SaslType {
	case conns.SASLType_SCRAM:
		return scram.Mechanism(scram.SHA512, connCfg.SaslUsername, connCfg.SaslPassword)
	case conns.SASLType_PLAIN:
		return plain.Mechanism{
			Username: connCfg.SaslUsername,
			Password: connCfg.SaslPassword,
		}, nil
	}

	return nil, nil
}

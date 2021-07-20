package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/kafka"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

// getBackendWrite gets the backend message bus needed to read/write from
// TODO: genericize after backend refactor
func (p *PlumberServer) getBackendWrite(req *protos.WriteRequest) (*kafka.KafkaWriter, error) {
	connCfg := p.getConn(req.ConnectionId)
	if connCfg == nil {
		return nil, errors.New("connection does not exist")
	}

	switch {
	case connCfg.GetKafka() != nil:
		return p.getBackendWriteKafka(connCfg.GetKafka())
	}

	return nil, errors.New("unknown message bus")
}

func (p *PlumberServer) getBackendWriteKafka(connCfg *conns.Kafka) (*kafka.KafkaWriter, error) {
	dialer := &skafka.Dialer{
		DualStack: true,
		Timeout:   time.Second * 10,
	}

	if connCfg.InsecureTls {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getKafkaAuthConfig(connCfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not get authentication mechanism")
	}

	dialer.SASLMechanism = auth

	// Attempt to establish connection on startup
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))

	// TODO: handle multiple brokers
	kafkaConn, err := dialer.DialContext(ctxDeadline, "tcp", connCfg.Address[0])
	if err != nil {
		logrus.Errorf("unable to create initial connection to broker '%s', trying next broker", connCfg.Address[0])
	}
	if err != nil {
		return nil, err
	}

	writer := skafka.NewWriter(skafka.WriterConfig{
		Brokers:   connCfg.Address,
		Dialer:    dialer,
		BatchSize: 1, // TODO: add to protos?
	})

	return &kafka.KafkaWriter{
		Writer: writer,
		Conn:   kafkaConn,
	}, nil
}

func (p *PlumberServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	backend, err := p.getBackendWrite(req)
	if err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	messages := make([]skafka.Message, 0)

	for _, v := range req.Records {
		km := v.GetKafka()
		messages = append(messages, skafka.Message{
			Topic:   km.Topic,
			Key:     km.Key,
			Value:   km.Value,
			Headers: convertProtoHeadersToKafka(km.GetHeaders()),
		})
	}

	backend.Writer.WriteMessages(ctx, messages...)

	return &protos.WriteResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   fmt.Sprintf("%d record(s) written", len(messages)),
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

// convertProtoHeadersToKafka converts type of header slice from segmentio's to our protobuf type
func convertProtoHeadersToKafka(original []*records.KafkaHeader) []skafka.Header {
	converted := make([]skafka.Header, 0)

	for _, o := range original {
		converted = append(converted, skafka.Header{
			Key:   o.Key,
			Value: []byte(o.Value),
		})
	}

	return converted
}

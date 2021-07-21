package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/kafka"
)

type Read struct {
	ID           string
	ContextCxl   context.Context
	CancelFunc   context.CancelFunc
	Backend      *kafka.KafkaReader // TODO: have to genercize once backend refactor is done
	ConnectionID string
	MessageCh    chan *skafka.Message // TODO: have to genercize this somehow
	log          *logrus.Entry
}

func (p *PlumberServer) StartRead(req *protos.StartReadRequest, srv protos.PlumberServer_StartReadServer) error {
	if err := p.validateRequest(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Reader needs a unique ID that frontend can reference
	readerID := uuid.NewV4().String()

	ctx, cancelFunc := context.WithCancel(context.Background())

	// TODO: figure out what we want to do with background reader? should this only be for writing to disk?
	// For now, just close reader when client disconnects from this endpoint
	defer cancelFunc()

	backend, err := p.getBackendRead(req)
	if err != nil {
		return CustomError(common.Code_ABORTED, err.Error())
	}

	cfg := req.GetKafka()

	// Launch reader and record
	reader := &Read{
		ID:           readerID,
		ContextCxl:   ctx,
		CancelFunc:   cancelFunc,
		Backend:      backend,
		ConnectionID: req.ConnectionId,
		MessageCh:    make(chan *skafka.Message, 100),
		log:          p.Log.WithField("read_id", readerID),
	}

	p.setRead(readerID, reader)

	go reader.Read()

	for {
		select {
		case msg := <-reader.MessageCh:
			messages := make([]*records.Message, 0)

			// TODO: batch these up and send multiple per response?
			messages = append(messages, p.generateKafkaPayload(cfg, msg))

			srv.Send(&protos.StartReadResponse{
				Status: &common.Status{
					Code:      common.Code_OK,
					Message:   "Message read",
					RequestId: requestID,
				},
				ReadId:   readerID,
				Messages: messages,
			})
		}
	}

	// TODO: what should we do when connection to the endpoint is ended (UI is closed)?
	// TODO: should we continue reading and filling up a channel or should we pause the read?

	return nil
}

// generateKafkaPayload generates a records.Message protobuf struct from a kafka message struct
func (p *PlumberServer) generateKafkaPayload(cfg *args.Kafka, msg *skafka.Message) *records.Message {

	// TODO: decode here

	return &records.Message{
		MessageId:        uuid.NewV4().String(),        // TODO: what to put here?
		PlumberId:        p.PersistentConfig.PlumberID, // TODO: generate and store UUID in plumber config so this is a static value
		UnixTimestampUtc: time.Now().UTC().UnixNano(),
		Message: &records.Message_Kafka{Kafka: &records.Kafka{
			Topic:     msg.Topic, // TODO: which topic?
			Key:       msg.Key,
			Value:     msg.Value, // TODO: decode
			Blob:      msg.Value,
			Timestamp: msg.Time.UTC().UnixNano(),
			Offset:    msg.Offset,
			Partition: int32(msg.Partition),
			Headers:   convertKafkaHeadersToProto(msg.Headers),
		}},
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

// Read is a goroutine that is launched when a read is started. It will continue running until plumber exiits
// or a read is stopped via the API
func (r *Read) Read() {
	defer r.Backend.Reader.Close()

	for {
		select {
		case <-r.ContextCxl.Done():
			r.log.Info("Read stopped")
			return
		default:
			// noop
		}

		msg, err := r.Backend.Reader.ReadMessage(r.ContextCxl)
		if err != nil && err != context.Canceled {
			r.log.Errorf("unable to read kafka message: %s", err)
			continue
		}

		r.MessageCh <- &msg
	}

}

func getKafkaAuthConfig(cfg *conns.Kafka) (sasl.Mechanism, error) {
	switch cfg.SaslType {
	case conns.SASLType_SCRAM:
		return scram.Mechanism(scram.SHA512, cfg.SaslUsername, cfg.SaslPassword)
	case conns.SASLType_PLAIN:
		return plain.Mechanism{
			Username: cfg.SaslUsername,
			Password: cfg.SaslPassword,
		}, nil
	}

	return nil, nil
}

// getBackendRead gets the backend message bus needed to read/write from
// TODO: genericize after backend refactor
func (p *PlumberServer) getBackendRead(req *protos.StartReadRequest) (*kafka.KafkaReader, error) {
	args := req.GetKafka()

	connCfg := p.getConn(req.ConnectionId)
	if connCfg == nil {
		return nil, errors.New("connection does not exist")
	}

	switch {
	case req.GetKafka() != nil:
		return p.getBackendReadKafka(connCfg, args)
	}

	return nil, errors.New("unknown message bus")
}

func (p *PlumberServer) getBackendReadKafka(connCfg *protos.Connection, args *args.Kafka) (*kafka.KafkaReader, error) {
	kafkaCfg := connCfg.GetKafka()

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

	// Attempt to establish connection on startup
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))

	kafkaConn, err := dialer.DialContext(ctxDeadline, "tcp", kafkaCfg.Address[0])
	if err != nil {
		logrus.Errorf("unable to create initial connection to broker '%s', trying next broker", kafkaCfg.Address[0])
	}
	if err != nil {
		return nil, err
	}

	commitInterval, _ := time.ParseDuration(fmt.Sprintf("%ds", args.CommitIntervalSeconds))
	maxWait, _ := time.ParseDuration(fmt.Sprintf("%ds", args.MaxWaitSeconds))
	rebalanceTimeout, _ := time.ParseDuration(fmt.Sprintf("%ds", args.RebalanceTimeoutSeconds))

	rc := skafka.ReaderConfig{
		Brokers:          kafkaCfg.Address,
		CommitInterval:   commitInterval,
		Dialer:           dialer,
		MaxWait:          maxWait,
		MinBytes:         int(args.MinBytes),
		MaxBytes:         int(args.MaxBytes),
		QueueCapacity:    100, // TODO: add to protos?
		RebalanceTimeout: rebalanceTimeout,
	}

	if args.UseConsumerGroup {
		rc.GroupTopics = args.Topics
		rc.GroupID = args.ConsumerGroupName
	} else {
		rc.Topic = args.Topics[0]
	}

	r := skafka.NewReader(rc)

	if !args.UseConsumerGroup {
		if err := r.SetOffset(args.ReadOffset); err != nil {
			return nil, errors.Wrap(err, "unable to set read offset")
		}
	}

	return &kafka.KafkaReader{
		Reader: r,
		Conn:   kafkaConn,
	}, nil
}

func (p *PlumberServer) StopRead(_ context.Context, req *protos.StopReadRequest) (*protos.StopReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Get reader and cancel
	read := p.getRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")

	}

	read.CancelFunc()

	return &protos.StopReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Message read",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

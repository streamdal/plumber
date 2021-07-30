package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/serializers"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

const SampleOffsetInterval = time.Second * 10 //time.Minute

type AttachedStream struct {
	MessageCh chan *records.Message
}

type Read struct {
	AttachedClientsMutex *sync.RWMutex
	AttachedClients      map[string]*AttachedStream
	PlumberID            string
	ID                   string
	Config               *protos.Read
	ContextCxl           context.Context
	CancelFunc           context.CancelFunc
	Backend              *kafka.KafkaReader // TODO: have to genercize once backend refactor is done
	MsgDesc              *desc.MessageDescriptor
	log                  *logrus.Entry
}

func (p *PlumberServer) GetAllReads(_ context.Context, req *protos.GetAllReadsRequest) (*protos.GetAllReadsResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	reads := make([]*protos.Read, 0)

	for _, v := range p.Reads {
		reads = append(reads, v.Config)
	}

	return &protos.GetAllReadsResponse{
		Read: reads,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) StreamRead(req *protos.StreamReadRequest, srv protos.PlumberServer_StreamReadServer) error {
	requestID := uuid.NewV4().String()

	if err := p.validateRequest(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if req.ReadId == "" {
		return CustomError(common.Code_FAILED_PRECONDITION, "read not found")
	}

	read := p.getRead(req.ReadId)
	if read == nil {
		return CustomError(common.Code_NOT_FOUND, "read not found")
	}

	stream := &AttachedStream{
		MessageCh: make(chan *records.Message, 1000),
	}

	read.AttachedClientsMutex.Lock()
	read.AttachedClients[requestID] = stream
	read.AttachedClientsMutex.Unlock()

	llog := p.Log.WithField("read_id", read.ID).
		WithField("client_id", requestID)

	// Ensure we remove this client from the active streams on exit
	defer func() {
		read.AttachedClientsMutex.Lock()
		delete(read.AttachedClients, requestID)
		read.AttachedClientsMutex.Unlock()
		llog.Debugf("Stream detached for '%s'", requestID)
	}()

	llog.Debugf("New stream attached")

	// Start reading
	for {
		select {
		case msg := <-stream.MessageCh:
			messages := make([]*records.Message, 0)

			// TODO: batch these up and send multiple per response?
			messages = append(messages, msg)

			res := &protos.StreamReadResponse{
				Status: &common.Status{
					Code:      common.Code_OK,
					Message:   "Message read",
					RequestId: requestID,
				},
				Messages: messages,
			}
			if err := srv.Send(res); err != nil {
				llog.Error(err)
				continue
			}
			llog.Debugf("Sent message to client '%s'", requestID)
		case <-read.ContextCxl.Done():
			// StartRead stopped. close out all streams for it
			llog.Debugf("StartRead stopped. closing stream for client '%s'", requestID)
			return nil
		default:
			// NOOP
		}
	}
}

func (p *PlumberServer) StartRead(_ context.Context, req *protos.StartReadRequest) (*protos.StartReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	readCfg := req.GetRead()
	if err := validateRead(readCfg); err != nil {
		return nil, err
	}

	requestID := uuid.NewV4().String()

	// Reader needs a unique ID that frontend can reference
	readerID := uuid.NewV4().String()

	md, err := generateMD(readCfg.DecodeOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	backend, err := p.getBackendRead(readCfg)
	if err != nil {
		cancelFunc()
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Launch read and record
	read := &Read{
		AttachedClients:      make(map[string]*AttachedStream, 0),
		AttachedClientsMutex: &sync.RWMutex{},
		PlumberID:            p.PersistentConfig.PlumberID,
		ID:                   readerID,
		Config:               readCfg,
		ContextCxl:           ctx,
		CancelFunc:           cancelFunc,
		Backend:              backend,
		MsgDesc:              md,
		log:                  p.Log.WithField("read_id", readerID),
	}

	p.setRead(readerID, read)

	var offsetStart, offsetStep int64

	// Can't wait forever. If no traffic on the topic after 2 minutes, cancel sample call
	timeoutCtx, _ := context.WithTimeout(context.Background(), SampleOffsetInterval*2)

	if readCfg.GetSampleOptions() != nil {
		offsetStep, offsetStart, err = read.GetSampleRate(timeoutCtx)
		if err != nil {
			return nil, CustomError(common.Code_ABORTED, "could not calculate sample rate: "+err.Error())
		}
	}

	go read.StartRead(offsetStart, offsetStep)

	return &protos.StartReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "StartRead started",
			RequestId: requestID,
		},
		ReadId: readerID,
	}, nil
}

// generateKafkaPayload generates a records.Message protobuf struct from a kafka message struct
func (r *Read) generateKafkaPayload(msg *skafka.Message) (*records.Message, error) {
	var err error
	var data []byte

	switch r.Config.DecodeOptions.Type {
	case encoding.Type_PROTOBUF:
		data, err = DecodeProtobuf(r.MsgDesc, msg.Value)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode protobuf")
		}
	case encoding.Type_AVRO:
		data, err = serializers.AvroDecode(r.Config.DecodeOptions.GetAvro().Schema, msg.Value)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode AVRO message")
		}
		fallthrough
	case encoding.Type_JSON_SCHEMA:
		// TODO
		fallthrough
	default:
		data = msg.Value
	}

	return &records.Message{
		MessageId:        uuid.NewV4().String(),
		PlumberId:        r.PlumberID,
		UnixTimestampUtc: time.Now().UTC().UnixNano(),
		Message: &records.Message_Kafka{Kafka: &records.Kafka{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     data,
			Blob:      msg.Value,
			Timestamp: msg.Time.UTC().UnixNano(),
			Offset:    msg.Offset,
			Partition: int32(msg.Partition),
			Headers:   convertKafkaHeadersToProto(msg.Headers),
		}},
	}, nil
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

// GetSampleRate gets the number of messages received in SampleOffsetInterval in order to calculate how many
func (r *Read) GetSampleRate(ctx context.Context) (offsetStep int64, offsetStart int64, err error) {
	if err := r.Backend.Reader.SetOffset(skafka.LastOffset); err != nil {
		return 0, 0, errors.Wrap(err, "unable to set latest offset")
	}

	r.log.Debug("starting sample rate calculation")

	msg, err := r.Backend.Reader.ReadMessage(ctx)
	if err != nil {
		if err == context.Canceled {
			err = errors.New("context cancelled, could not get sample rate")
			return 0, 0, err
		} else {
			err = fmt.Errorf("unable to read kafka message: %s", err)
			return 0, 0, err
		}
	}

	offsetStart = msg.Offset

	r.log.Debugf("Got first offset: %d", offsetStart)

	time.Sleep(SampleOffsetInterval)

	if err := r.Backend.Reader.SetOffset(skafka.LastOffset); err != nil {
		return 0, 0, errors.Wrap(err, "unable to set latest offset")
	}

	msg, err = r.Backend.Reader.ReadMessage(ctx)
	if err != nil {
		if err == context.Canceled {
			err = errors.New("context cancelled, could not get sample rate")
			return 0, 0, err
		} else {
			err = fmt.Errorf("unable to read kafka message: %s", err)
			return 0, 0, err
		}
	}

	span := float64(msg.Offset - offsetStart)

	sampleOpts := r.Config.SampleOptions

	var rate float64
	switch sampleOpts.SampleInterval {
	case protos.SampleOptions_MINUTE:
		rate = float64(sampleOpts.SampleRate)
	case protos.SampleOptions_SECOND:
		rate = float64(sampleOpts.SampleRate) * 60
	default:
		return 0, 0, fmt.Errorf("unknown sample interval: '%d'", sampleOpts.SampleInterval)
	}

	offsetStep = int64(math.Round(span / rate))

	r.log.Debugf("Calculated offsetStep (offset %d - offset %d) / rate %d = %d",
		msg.Offset, offsetStart, int(rate), offsetStep)

	return offsetStep, offsetStart, nil
}

// StartRead is a goroutine that is launched when a read is started. It will continue running until plumber exits
// or a read is stopped via the API
func (r *Read) StartRead(offsetStart, offsetStep int64) {
	defer r.Backend.Reader.Close()

	if offsetStart > 0 {
		r.log.Debugf("Starting read at %d with step %d", offsetStart, offsetStep)
	}

	for {
		select {
		case <-r.ContextCxl.Done():
			r.log.Info("StartRead stopped")
			return
		default:
			// noop
		}

		var err error

		if offsetStart > 0 {
			r.Backend.Reader.SetOffset(offsetStart)
		}

		msg, err := r.Backend.Reader.ReadMessage(r.ContextCxl)
		if err != nil {
			if err == context.Canceled {
				return
			} else {
				r.log.Errorf("unable to read kafka message: %s", err)
				continue
			}
		}

		payload, err := r.generateKafkaPayload(&msg)
		if err != nil {
			r.log.Errorf("unable to generate kafka payload: %s", err)
		}

		// Send message payload to all attached streams
		r.AttachedClientsMutex.RLock()
		for id, s := range r.AttachedClients {
			r.log.Debugf("StartRead message to stream '%s'", id)
			s.MessageCh <- payload
		}
		r.AttachedClientsMutex.RUnlock()

		// Sampled read, increment offset offset
		if offsetStart > 0 {
			offsetStart += offsetStep
		}
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
func (p *PlumberServer) getBackendRead(read *protos.Read) (*kafka.KafkaReader, error) {
	connCfg := p.getConn(read.ConnectionId)
	if connCfg == nil {
		return nil, errors.New("connection does not exist")
	}

	switch {
	case read.GetKafka() != nil:
		args := read.GetKafka()
		samp := read.GetSampleOptions()
		return p.getBackendReadKafka(connCfg.Connection, args, samp)
	}

	return nil, errors.New("unknown message bus")
}

func (p *PlumberServer) getBackendReadKafka(connCfg *protos.Connection, args *args.Kafka, samp *protos.SampleOptions) (*kafka.KafkaReader, error) {
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

	commitInterval, err := time.ParseDuration(fmt.Sprintf("%ds", args.CommitIntervalSeconds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse CommitIntervalSeconds")
	}

	maxWait, err := time.ParseDuration(fmt.Sprintf("%ds", args.MaxWaitSeconds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse MaxWaitSeconds")
	}

	rebalanceTimeout, err := time.ParseDuration(fmt.Sprintf("%ds", args.RebalanceTimeoutSeconds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse RebalanceTimeoutSeconds")
	}

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

	if samp != nil {
		// Sampling mode. No consumer group
		rc.Topic = args.Topics[0]
		rc.StartOffset = args.ReadOffset
	} else {
		// Non sampling mode
		if args.UseConsumerGroup {
			rc.GroupTopics = args.Topics
			rc.GroupID = args.ConsumerGroupName
		} else {
			rc.Topic = args.Topics[0]
			rc.StartOffset = args.ReadOffset
		}
	}

	r := skafka.NewReader(rc)

	return &kafka.KafkaReader{
		Reader: r,
	}, nil
}

func (p *PlumberServer) StopRead(_ context.Context, req *protos.StopReadRequest) (*protos.StopReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := p.getRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	read.CancelFunc()

	p.Log.WithField("request_id", requestID).Infof("StartRead '%s' stopped", req.ReadId)

	return &protos.StopReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Message read",
			RequestId: requestID,
		},
	}, nil
}

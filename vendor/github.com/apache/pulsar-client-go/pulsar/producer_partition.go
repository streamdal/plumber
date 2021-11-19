// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	internalcrypto "github.com/apache/pulsar-client-go/pulsar/internal/crypto"

	"github.com/gogo/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"

	ua "go.uber.org/atomic"
)

type producerState int32

const (
	// producer states
	producerInit = iota
	producerReady
	producerClosing
	producerClosed
)

var (
	errFailAddToBatch  = newError(AddToBatchFailed, "message add to batch failed")
	errSendTimeout     = newError(TimeoutError, "message send timeout")
	errSendQueueIsFull = newError(ProducerQueueIsFull, "producer send queue is full")
	errContextExpired  = newError(TimeoutError, "message send context expired")
	errMessageTooLarge = newError(MessageTooBig, "message size exceeds MaxMessageSize")
	errProducerClosed  = newError(ProducerClosed, "producer already been closed")

	buffersPool sync.Pool
)

var errTopicNotFount = "TopicNotFound"

type partitionProducer struct {
	state  ua.Int32
	client *client
	topic  string
	log    log.Logger
	cnx    internal.Connection

	options                  *ProducerOptions
	producerName             string
	userProvidedProducerName bool
	producerID               uint64
	batchBuilder             internal.BatchBuilder
	sequenceIDGenerator      *uint64
	batchFlushTicker         *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan      chan interface{}
	connectClosedCh chan connectionClosed

	publishSemaphore internal.Semaphore
	pendingQueue     internal.BlockingQueue
	lastSequenceID   int64
	schemaInfo       *SchemaInfo
	partitionIdx     int32
	metrics          *internal.LeveledMetrics

	epoch uint64
}

func newPartitionProducer(client *client, topic string, options *ProducerOptions, partitionIdx int,
	metrics *internal.LeveledMetrics) (
	*partitionProducer, error) {
	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	var maxPendingMessages int
	if options.MaxPendingMessages == 0 {
		maxPendingMessages = 1000
	} else {
		maxPendingMessages = options.MaxPendingMessages
	}

	logger := client.log.SubLogger(log.Fields{"topic": topic})

	p := &partitionProducer{
		client:           client,
		topic:            topic,
		log:              logger,
		options:          options,
		producerID:       client.rpcClient.NewProducerID(),
		eventsChan:       make(chan interface{}, maxPendingMessages),
		connectClosedCh:  make(chan connectionClosed, 10),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
		publishSemaphore: internal.NewSemaphore(int32(maxPendingMessages)),
		pendingQueue:     internal.NewBlockingQueue(maxPendingMessages),
		lastSequenceID:   -1,
		partitionIdx:     int32(partitionIdx),
		metrics:          metrics,
		epoch:            0,
	}
	p.setProducerState(producerInit)

	if options.Schema != nil && options.Schema.GetSchemaInfo() != nil {
		p.schemaInfo = options.Schema.GetSchemaInfo()
	} else {
		p.schemaInfo = nil
	}

	if options.Name != "" {
		p.producerName = options.Name
		p.userProvidedProducerName = true
	} else {
		p.userProvidedProducerName = false
	}

	encryption := options.Encryption
	// add default message crypto if not provided
	if encryption != nil && len(encryption.Keys) > 0 {
		if encryption.KeyReader == nil {
			return nil, fmt.Errorf("encryption is enabled, KeyReader can not be nil")
		}

		if encryption.MessageCrypto == nil {
			logCtx := fmt.Sprintf("[%v] [%v] [%v]", p.topic, p.producerName, p.producerID)
			messageCrypto, err := crypto.NewDefaultMessageCrypto(logCtx, true, logger)
			if err != nil {
				logger.WithError(err).Error("Unable to get MessageCrypto instance. Producer creation is abandoned")
				return nil, err
			}
			p.options.Encryption.MessageCrypto = messageCrypto
		}
	}

	err := p.grabCnx()
	if err != nil {
		logger.WithError(err).Error("Failed to create producer")
		return nil, err
	}

	p.log = p.log.SubLogger(log.Fields{
		"producer_name": p.producerName,
		"producerID":    p.producerID,
	})

	p.log.WithField("cnx", p.cnx.ID()).Info("Created producer")
	p.setProducerState(producerReady)

	if p.options.SendTimeout > 0 {
		go p.failTimeoutMessages()
	}

	go p.runEventsLoop()

	return p, nil
}

func (p *partitionProducer) grabCnx() error {
	lr, err := p.client.lookupService.Lookup(p.topic)
	if err != nil {
		p.log.WithError(err).Warn("Failed to lookup topic")
		return err
	}

	p.log.Debug("Lookup result: ", lr)
	id := p.client.rpcClient.NewRequestID()

	// set schema info for producer

	pbSchema := new(pb.Schema)
	if p.schemaInfo != nil {
		tmpSchemaType := pb.Schema_Type(int32(p.schemaInfo.Type))
		pbSchema = &pb.Schema{
			Name:       proto.String(p.schemaInfo.Name),
			Type:       &tmpSchemaType,
			SchemaData: []byte(p.schemaInfo.Schema),
			Properties: internal.ConvertFromStringMap(p.schemaInfo.Properties),
		}
		p.log.Debugf("The partition consumer schema name is: %s", pbSchema.Name)
	} else {
		pbSchema = nil
		p.log.Debug("The partition consumer schema is nil")
	}

	cmdProducer := &pb.CommandProducer{
		RequestId:                proto.Uint64(id),
		Topic:                    proto.String(p.topic),
		Encrypted:                nil,
		ProducerId:               proto.Uint64(p.producerID),
		Schema:                   pbSchema,
		Epoch:                    proto.Uint64(atomic.LoadUint64(&p.epoch)),
		UserProvidedProducerName: proto.Bool(p.userProvidedProducerName),
	}

	if p.producerName != "" {
		cmdProducer.ProducerName = proto.String(p.producerName)
	}

	if len(p.options.Properties) > 0 {
		cmdProducer.Metadata = toKeyValues(p.options.Properties)
	}
	res, err := p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_PRODUCER, cmdProducer)
	if err != nil {
		p.log.WithError(err).Error("Failed to create producer")
		return err
	}

	p.producerName = res.Response.ProducerSuccess.GetProducerName()

	var encryptor internalcrypto.Encryptor
	if p.options.Encryption != nil {
		encryptor = internalcrypto.NewProducerEncryptor(p.options.Encryption.Keys,
			p.options.Encryption.KeyReader,
			p.options.Encryption.MessageCrypto,
			p.options.Encryption.ProducerCryptoFailureAction, p.log)
	} else {
		encryptor = internalcrypto.NewNoopEncryptor()
	}

	if p.options.DisableBatching {
		provider, _ := GetBatcherBuilderProvider(DefaultBatchBuilder)
		p.batchBuilder, err = provider(p.options.BatchingMaxMessages, p.options.BatchingMaxSize,
			p.producerName, p.producerID, pb.CompressionType(p.options.CompressionType),
			compression.Level(p.options.CompressionLevel),
			p,
			p.log,
			encryptor)
		if err != nil {
			return err
		}
	} else if p.batchBuilder == nil {
		provider, err := GetBatcherBuilderProvider(p.options.BatcherBuilderType)
		if err != nil {
			provider, _ = GetBatcherBuilderProvider(DefaultBatchBuilder)
		}

		p.batchBuilder, err = provider(p.options.BatchingMaxMessages, p.options.BatchingMaxSize,
			p.producerName, p.producerID, pb.CompressionType(p.options.CompressionType),
			compression.Level(p.options.CompressionLevel),
			p,
			p.log,
			encryptor)
		if err != nil {
			return err
		}
	}

	if p.sequenceIDGenerator == nil {
		nextSequenceID := uint64(res.Response.ProducerSuccess.GetLastSequenceId() + 1)
		p.sequenceIDGenerator = &nextSequenceID
	}
	p.cnx = res.Cnx
	p.cnx.RegisterListener(p.producerID, p)
	p.log.WithFields(log.Fields{
		"cnx":   res.Cnx.ID(),
		"epoch": atomic.LoadUint64(&p.epoch),
	}).Debug("Connected producer")

	pendingItems := p.pendingQueue.ReadableSlice()
	viewSize := len(pendingItems)
	if viewSize > 0 {
		p.log.Infof("Resending %d pending batches", viewSize)
		lastViewItem := pendingItems[viewSize-1].(*pendingItem)

		// iterate at most pending items
		for i := 0; i < viewSize; i++ {
			item := p.pendingQueue.Poll()
			if item == nil {
				continue
			}
			pi := item.(*pendingItem)
			// when resending pending batches, we update the sendAt timestamp and put to the back of queue
			// to avoid pending item been removed by failTimeoutMessages and cause race condition
			pi.Lock()
			pi.sentAt = time.Now()
			pi.Unlock()
			p.pendingQueue.Put(pi)
			p.cnx.WriteData(pi.batchData)

			if pi == lastViewItem {
				break
			}
		}
	}
	return nil
}

type connectionClosed struct{}

func (p *partitionProducer) GetBuffer() internal.Buffer {
	b, ok := buffersPool.Get().(internal.Buffer)
	if ok {
		b.Clear()
	}
	return b
}

func (p *partitionProducer) ConnectionClosed() {
	// Trigger reconnection in the produce goroutine
	p.log.WithField("cnx", p.cnx.ID()).Warn("Connection was closed")
	p.connectClosedCh <- connectionClosed{}
}

func (p *partitionProducer) reconnectToBroker() {
	var (
		maxRetry int
		backoff  = internal.Backoff{}
	)

	if p.options.MaxReconnectToBroker == nil {
		maxRetry = -1
	} else {
		maxRetry = int(*p.options.MaxReconnectToBroker)
	}

	for maxRetry != 0 {
		if p.getProducerState() != producerReady {
			// Producer is already closing
			return
		}

		d := backoff.Next()
		p.log.Info("Reconnecting to broker in ", d)
		time.Sleep(d)
		atomic.AddUint64(&p.epoch, 1)
		err := p.grabCnx()
		if err == nil {
			// Successfully reconnected
			p.log.WithField("cnx", p.cnx.ID()).Info("Reconnected producer to broker")
			return
		}
		errMsg := err.Error()
		if strings.Contains(errMsg, errTopicNotFount) {
			// when topic is deleted, we should give up reconnection.
			p.log.Warn("Topic Not Found.")
			break
		}

		if maxRetry > 0 {
			maxRetry--
		}
	}
}

func (p *partitionProducer) runEventsLoop() {
	for {
		select {
		case i := <-p.eventsChan:
			switch v := i.(type) {
			case *sendRequest:
				p.internalSend(v)
			case *flushRequest:
				p.internalFlush(v)
			case *closeProducer:
				p.internalClose(v)
				return
			}
		case <-p.connectClosedCh:
			p.reconnectToBroker()
		case <-p.batchFlushTicker.C:
			if p.batchBuilder.IsMultiBatches() {
				p.internalFlushCurrentBatches()
			} else {
				p.internalFlushCurrentBatch()
			}
		}
	}
}

func (p *partitionProducer) Topic() string {
	return p.topic
}

func (p *partitionProducer) Name() string {
	return p.producerName
}

func (p *partitionProducer) internalSend(request *sendRequest) {
	p.log.Debug("Received send request: ", *request)

	msg := request.msg

	payload := msg.Payload
	var schemaPayload []byte
	var err error
	if p.options.Schema != nil {
		schemaPayload, err = p.options.Schema.Encode(msg.Value)
		if err != nil {
			p.log.WithError(err).Errorf("Schema encode message failed %s", msg.Value)
			return
		}
	}

	if payload == nil {
		payload = schemaPayload
	}

	// if msg is too large
	if len(payload) > int(p.cnx.GetMaxMessageSize()) {
		p.publishSemaphore.Release()
		request.callback(nil, request.msg, errMessageTooLarge)
		p.log.WithError(errMessageTooLarge).
			WithField("size", len(payload)).
			WithField("properties", msg.Properties).
			Errorf("MaxMessageSize %d", int(p.cnx.GetMaxMessageSize()))
		p.metrics.PublishErrorsMsgTooLarge.Inc()
		return
	}

	deliverAt := msg.DeliverAt
	if msg.DeliverAfter.Nanoseconds() > 0 {
		deliverAt = time.Now().Add(msg.DeliverAfter)
	}

	sendAsBatch := !p.options.DisableBatching &&
		msg.ReplicationClusters == nil &&
		deliverAt.UnixNano() < 0

	smm := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int(len(payload)),
	}

	if msg.EventTime.UnixNano() != 0 {
		smm.EventTime = proto.Uint64(internal.TimestampMillis(msg.EventTime))
	}

	if msg.Key != "" {
		smm.PartitionKey = proto.String(msg.Key)
	}

	if len(msg.OrderingKey) != 0 {
		smm.OrderingKey = []byte(msg.OrderingKey)
	}

	if msg.Properties != nil {
		smm.Properties = internal.ConvertFromStringMap(msg.Properties)
	}

	if msg.SequenceID != nil {
		sequenceID := uint64(*msg.SequenceID)
		smm.SequenceId = proto.Uint64(sequenceID)
	}

	if !sendAsBatch {
		p.internalFlushCurrentBatch()
	}

	if msg.DisableReplication {
		msg.ReplicationClusters = []string{"__local__"}
	}

	added := p.batchBuilder.Add(smm, p.sequenceIDGenerator, payload, request,
		msg.ReplicationClusters, deliverAt)
	if !added {
		// The current batch is full.. flush it and retry
		if p.batchBuilder.IsMultiBatches() {
			p.internalFlushCurrentBatches()
		} else {
			p.internalFlushCurrentBatch()
		}

		// after flushing try again to add the current payload
		if ok := p.batchBuilder.Add(smm, p.sequenceIDGenerator, payload, request,
			msg.ReplicationClusters, deliverAt); !ok {
			p.publishSemaphore.Release()
			request.callback(nil, request.msg, errFailAddToBatch)
			p.log.WithField("size", len(payload)).
				WithField("properties", msg.Properties).
				Error("unable to add message to batch")
			return
		}
	}

	if !sendAsBatch || request.flushImmediately {
		if p.batchBuilder.IsMultiBatches() {
			p.internalFlushCurrentBatches()
		} else {
			p.internalFlushCurrentBatch()
		}
	}
}

type pendingItem struct {
	sync.Mutex
	batchData    internal.Buffer
	sequenceID   uint64
	sentAt       time.Time
	sendRequests []interface{}
	completed    bool
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	batchData, sequenceID, callbacks, err := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	// error occurred in batch flush
	// report it using callback
	if err != nil {
		for _, cb := range callbacks {
			if sr, ok := cb.(*sendRequest); ok {
				sr.callback(nil, sr.msg, err)
			}
		}
		return
	}

	p.pendingQueue.Put(&pendingItem{
		sentAt:       time.Now(),
		batchData:    batchData,
		sequenceID:   sequenceID,
		sendRequests: callbacks,
	})
	p.cnx.WriteData(batchData)
}

func (p *partitionProducer) failTimeoutMessages() {
	diff := func(sentAt time.Time) time.Duration {
		return p.options.SendTimeout - time.Since(sentAt)
	}

	t := time.NewTimer(p.options.SendTimeout)
	defer t.Stop()

	for range t.C {
		state := p.getProducerState()
		if state == producerClosing || state == producerClosed {
			return
		}

		item := p.pendingQueue.Peek()
		if item == nil {
			// pending queue is empty
			t.Reset(p.options.SendTimeout)
			continue
		}
		oldestItem := item.(*pendingItem)
		if nextWaiting := diff(oldestItem.sentAt); nextWaiting > 0 {
			// none of these pending messages have timed out, wait and retry
			t.Reset(nextWaiting)
			continue
		}

		// since pending queue is not thread safe because of there is no global iteration lock
		// to control poll from pending queue, current goroutine and connection receipt handler
		// iterate pending queue at the same time, this maybe a performance trade-off
		// see https://github.com/apache/pulsar-client-go/pull/301
		curViewItems := p.pendingQueue.ReadableSlice()
		viewSize := len(curViewItems)
		if viewSize <= 0 {
			// double check
			t.Reset(p.options.SendTimeout)
			continue
		}
		p.log.Infof("Failing %d messages", viewSize)
		lastViewItem := curViewItems[viewSize-1].(*pendingItem)

		// iterate at most viewSize items
		for i := 0; i < viewSize; i++ {
			tickerNeedWaiting := time.Duration(0)
			item := p.pendingQueue.CompareAndPoll(
				func(m interface{}) bool {
					if m == nil {
						return false
					}

					pi := m.(*pendingItem)
					pi.Lock()
					defer pi.Unlock()
					if nextWaiting := diff(pi.sentAt); nextWaiting > 0 {
						// current and subsequent items not timeout yet, stop iterating
						tickerNeedWaiting = nextWaiting
						return false
					}
					return true
				})

			if item == nil {
				t.Reset(p.options.SendTimeout)
				break
			}

			if tickerNeedWaiting > 0 {
				t.Reset(tickerNeedWaiting)
				break
			}

			pi := item.(*pendingItem)
			pi.Lock()

			for _, i := range pi.sendRequests {
				sr := i.(*sendRequest)
				if sr.msg != nil {
					size := len(sr.msg.Payload)
					p.publishSemaphore.Release()
					p.metrics.MessagesPending.Dec()
					p.metrics.BytesPending.Sub(float64(size))
					p.metrics.PublishErrorsTimeout.Inc()
					p.log.WithError(errSendTimeout).
						WithField("size", size).
						WithField("properties", sr.msg.Properties)
				}
				if sr.callback != nil {
					sr.callback(nil, sr.msg, errSendTimeout)
				}
			}

			// flag the send has completed with error, flush make no effect
			pi.Complete()
			pi.Unlock()

			// finally reached the last view item, current iteration ends
			if pi == lastViewItem {
				t.Reset(p.options.SendTimeout)
				break
			}
		}
	}
}

func (p *partitionProducer) internalFlushCurrentBatches() {
	batchesData, sequenceIDs, callbacks, errors := p.batchBuilder.FlushBatches()
	if batchesData == nil {
		return
	}

	for i := range batchesData {
		// error occurred in processing batch
		// report it using callback
		if errors[i] != nil {
			for _, cb := range callbacks[i] {
				if sr, ok := cb.(*sendRequest); ok {
					sr.callback(nil, sr.msg, errors[i])
				}
			}
			continue
		}
		if batchesData[i] == nil {
			continue
		}
		p.pendingQueue.Put(&pendingItem{
			sentAt:       time.Now(),
			batchData:    batchesData[i],
			sequenceID:   sequenceIDs[i],
			sendRequests: callbacks[i],
		})
		p.cnx.WriteData(batchesData[i])
	}

}

func (p *partitionProducer) internalFlush(fr *flushRequest) {
	if p.batchBuilder.IsMultiBatches() {
		p.internalFlushCurrentBatches()
	} else {
		p.internalFlushCurrentBatch()
	}

	pi, ok := p.pendingQueue.PeekLast().(*pendingItem)
	if !ok {
		fr.waitGroup.Done()
		return
	}

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	defer pi.Unlock()

	if pi.completed {
		// The last item in the queue has been completed while we were
		// looking at it. It's safe at this point to assume that every
		// message enqueued before Flush() was called are now persisted
		fr.waitGroup.Done()
		return
	}

	sendReq := &sendRequest{
		msg: nil,
		callback: func(id MessageID, message *ProducerMessage, e error) {
			fr.err = e
			fr.waitGroup.Done()
		},
	}

	pi.sendRequests = append(pi.sendRequests, sendReq)
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	var err error
	var msgID MessageID

	p.internalSendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		err = e
		msgID = ID
		wg.Done()
	}, true)

	wg.Wait()
	return msgID, err
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	p.internalSendAsync(ctx, msg, callback, false)
}

func (p *partitionProducer) internalSendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error), flushImmediately bool) {
	if p.getProducerState() != producerReady {
		// Producer is closing
		callback(nil, msg, errProducerClosed)
		return
	}

	sr := &sendRequest{
		ctx:              ctx,
		msg:              msg,
		callback:         callback,
		flushImmediately: flushImmediately,
		publishTime:      time.Now(),
	}
	p.options.Interceptors.BeforeSend(p, msg)

	if p.options.DisableBlockIfQueueFull {
		if !p.publishSemaphore.TryAcquire() {
			if callback != nil {
				callback(nil, msg, errSendQueueIsFull)
			}
			return
		}
	} else {
		if !p.publishSemaphore.Acquire(ctx) {
			callback(nil, msg, errContextExpired)
			return
		}
	}

	p.metrics.MessagesPending.Inc()
	p.metrics.BytesPending.Add(float64(len(sr.msg.Payload)))

	p.eventsChan <- sr
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {
	pi, ok := p.pendingQueue.Peek().(*pendingItem)

	if !ok {
		// if we receive a receipt although the pending queue is empty, the state of the broker and the producer differs.
		// At that point, it is better to close the connection to the broker to reconnect to a broker hopping it solves
		// the state discrepancy.
		p.log.Warnf("Received ack for %v although the pending queue is empty, closing connection", response.GetMessageId())
		p.cnx.Close()
		return
	}

	if pi.sequenceID != response.GetSequenceId() {
		// if we receive a receipt that is not the one expected, the state of the broker and the producer differs.
		// At that point, it is better to close the connection to the broker to reconnect to a broker hopping it solves
		// the state discrepancy.
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v, closing connection", response.GetMessageId(),
			response.GetSequenceId(), pi.sequenceID)
		p.cnx.Close()
		return
	}

	// The ack was indeed for the expected item in the queue, we can remove it and trigger the callback
	p.pendingQueue.Poll()

	now := time.Now().UnixNano()

	// lock the pending item while sending the requests
	pi.Lock()
	defer pi.Unlock()
	p.metrics.PublishRPCLatency.Observe(float64(now-pi.sentAt.UnixNano()) / 1.0e9)
	for idx, i := range pi.sendRequests {
		sr := i.(*sendRequest)
		if sr.msg != nil {
			atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))
			p.publishSemaphore.Release()

			p.metrics.PublishLatency.Observe(float64(now-sr.publishTime.UnixNano()) / 1.0e9)
			p.metrics.MessagesPublished.Inc()
			p.metrics.MessagesPending.Dec()
			payloadSize := float64(len(sr.msg.Payload))
			p.metrics.BytesPublished.Add(payloadSize)
			p.metrics.BytesPending.Sub(payloadSize)
		}

		if sr.callback != nil || len(p.options.Interceptors) > 0 {
			msgID := newMessageID(
				int64(response.MessageId.GetLedgerId()),
				int64(response.MessageId.GetEntryId()),
				int32(idx),
				p.partitionIdx,
			)

			if sr.callback != nil {
				sr.callback(msgID, sr.msg, nil)
			}

			p.options.Interceptors.OnSendAcknowledgement(p, sr.msg, msgID)
		}
	}

	// Mark this pending item as done
	pi.Complete()
}

func (p *partitionProducer) internalClose(req *closeProducer) {
	defer req.waitGroup.Done()
	if !p.casProducerState(producerReady, producerClosing) {
		return
	}

	p.log.Info("Closing producer")

	id := p.client.rpcClient.NewRequestID()
	_, err := p.client.rpcClient.RequestOnCnx(p.cnx, id, pb.BaseCommand_CLOSE_PRODUCER, &pb.CommandCloseProducer{
		ProducerId: &p.producerID,
		RequestId:  &id,
	})

	if err != nil {
		p.log.WithError(err).Warn("Failed to close producer")
	} else {
		p.log.Info("Closed producer")
	}

	if err = p.batchBuilder.Close(); err != nil {
		p.log.WithError(err).Warn("Failed to close batch builder")
	}

	p.setProducerState(producerClosed)
	p.cnx.UnregisterListener(p.producerID)
	p.batchFlushTicker.Stop()
}

func (p *partitionProducer) LastSequenceID() int64 {
	return atomic.LoadInt64(&p.lastSequenceID)
}

func (p *partitionProducer) Flush() error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &flushRequest{&wg, nil}
	p.eventsChan <- cp

	wg.Wait()
	return cp.err
}

func (p *partitionProducer) getProducerState() producerState {
	return producerState(p.state.Load())
}

func (p *partitionProducer) setProducerState(state producerState) {
	p.state.Swap(int32(state))
}

// set a new consumerState and return the last state
// returns bool if the new state has been set or not
func (p *partitionProducer) casProducerState(oldState, newState producerState) bool {
	return p.state.CAS(int32(oldState), int32(newState))
}

func (p *partitionProducer) Close() {
	if p.getProducerState() != producerReady {
		// Producer is closing
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &closeProducer{&wg}
	p.eventsChan <- cp

	wg.Wait()
}

type sendRequest struct {
	ctx              context.Context
	msg              *ProducerMessage
	callback         func(MessageID, *ProducerMessage, error)
	publishTime      time.Time
	flushImmediately bool
}

type closeProducer struct {
	waitGroup *sync.WaitGroup
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}

func (i *pendingItem) Complete() {
	if i.completed {
		return
	}
	i.completed = true
	buffersPool.Put(i.batchData)
}

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
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	defaultReceiverQueueSize = 1000
)

type reader struct {
	sync.Mutex
	pc                  *partitionConsumer
	messageCh           chan ConsumerMessage
	lastMessageInBroker trackingMessageID
	log                 log.Logger
	metrics             *internal.TopicMetrics
}

func newReader(client *client, options ReaderOptions) (Reader, error) {
	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is required")
	}

	if options.StartMessageID == nil {
		return nil, newError(InvalidConfiguration, "StartMessageID is required")
	}

	startMessageID, ok := toTrackingMessageID(options.StartMessageID)
	if !ok {
		// a custom type satisfying MessageID may not be a messageID or trackingMessageID
		// so re-create messageID using its data
		deserMsgID, err := deserializeMessageID(options.StartMessageID.Serialize())
		if err != nil {
			return nil, err
		}
		// de-serialized MessageID is a messageID
		startMessageID = trackingMessageID{
			messageID:    deserMsgID.(messageID),
			receivedTime: time.Now(),
		}
	}

	subscriptionName := options.SubscriptionRolePrefix
	if subscriptionName == "" {
		subscriptionName = "reader"
	}
	subscriptionName += "-" + generateRandomName()

	receiverQueueSize := options.ReceiverQueueSize
	if receiverQueueSize <= 0 {
		receiverQueueSize = defaultReceiverQueueSize
	}

	consumerOptions := &partitionConsumerOpts{
		topic:                      options.Topic,
		consumerName:               options.Name,
		subscription:               subscriptionName,
		subscriptionType:           Exclusive,
		receiverQueueSize:          receiverQueueSize,
		startMessageID:             startMessageID,
		startMessageIDInclusive:    options.StartMessageIDInclusive,
		subscriptionMode:           nonDurable,
		readCompacted:              options.ReadCompacted,
		metadata:                   options.Properties,
		nackRedeliveryDelay:        defaultNackRedeliveryDelay,
		replicateSubscriptionState: false,
	}

	reader := &reader{
		messageCh: make(chan ConsumerMessage),
		log:       client.log.SubLogger(log.Fields{"topic": options.Topic}),
		metrics:   client.metrics.GetTopicMetrics(options.Topic),
	}

	// Provide dummy dlq router with not dlq policy
	dlq, err := newDlqRouter(client, nil, client.log)
	if err != nil {
		return nil, err
	}

	pc, err := newPartitionConsumer(nil, client, consumerOptions, reader.messageCh, dlq, reader.metrics)
	if err != nil {
		close(reader.messageCh)
		return nil, err
	}

	reader.pc = pc
	reader.metrics.ReadersOpened.Inc()
	return reader, nil
}

func (r *reader) Topic() string {
	return r.pc.topic
}

func (r *reader) Next(ctx context.Context) (Message, error) {
	for {
		select {
		case cm, ok := <-r.messageCh:
			if !ok {
				return nil, newError(ConsumerClosed, "consumer closed")
			}

			// Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
			// it will specify the subscription position anyway
			msgID := cm.Message.ID()
			if mid, ok := toTrackingMessageID(msgID); ok {
				r.pc.lastDequeuedMsg = mid
				r.pc.AckID(mid)
				return cm.Message, nil
			}
			return nil, newError(InvalidMessage, fmt.Sprintf("invalid message id type %T", msgID))
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *reader) HasNext() bool {
	if !r.lastMessageInBroker.Undefined() && r.hasMoreMessages() {
		return true
	}

	for {
		lastMsgID, err := r.pc.getLastMessageID()
		if err != nil {
			r.log.WithError(err).Error("Failed to get last message id from broker")
			continue
		} else {
			r.lastMessageInBroker = lastMsgID
			break
		}
	}

	return r.hasMoreMessages()
}

func (r *reader) hasMoreMessages() bool {
	if !r.pc.lastDequeuedMsg.Undefined() {
		return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greater(r.pc.lastDequeuedMsg.messageID)
	}

	if r.pc.options.startMessageIDInclusive {
		return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greaterEqual(r.pc.startMessageID.messageID)
	}

	// Non-inclusive
	return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greater(r.pc.startMessageID.messageID)
}

func (r *reader) Close() {
	r.pc.Close()
	r.metrics.ReadersClosed.Inc()
}

func (r *reader) messageID(msgID MessageID) (trackingMessageID, bool) {
	mid, ok := toTrackingMessageID(msgID)
	if !ok {
		r.log.Warnf("invalid message id type %T", msgID)
		return trackingMessageID{}, false
	}

	partition := int(mid.partitionIdx)
	// did we receive a valid partition index?
	if partition < 0 {
		r.log.Warnf("invalid partition index %d expected", partition)
		return trackingMessageID{}, false
	}

	return mid, true
}

func (r *reader) Seek(msgID MessageID) error {
	r.Lock()
	defer r.Unlock()

	mid, ok := r.messageID(msgID)
	if !ok {
		return nil
	}

	return r.pc.Seek(mid)
}

func (r *reader) SeekByTime(time time.Time) error {
	r.Lock()
	defer r.Unlock()

	return r.pc.SeekByTime(time)
}

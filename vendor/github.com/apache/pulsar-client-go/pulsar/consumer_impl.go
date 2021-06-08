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
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const defaultNackRedeliveryDelay = 1 * time.Minute

type acker interface {
	AckID(id trackingMessageID)
	NackID(id trackingMessageID)
}

type consumer struct {
	sync.Mutex
	topic                     string
	client                    *client
	options                   ConsumerOptions
	consumers                 []*partitionConsumer
	consumerName              string
	disableForceTopicCreation bool

	// channel used to deliver message to clients
	messageCh chan ConsumerMessage

	dlq           *dlqRouter
	rlq           *retryRouter
	closeOnce     sync.Once
	closeCh       chan struct{}
	errorCh       chan error
	stopDiscovery func()

	log     log.Logger
	metrics *internal.TopicMetrics
}

func newConsumer(client *client, options ConsumerOptions) (Consumer, error) {
	if options.Topic == "" && options.Topics == nil && options.TopicsPattern == "" {
		return nil, newError(TopicNotFound, "topic is required")
	}

	if options.SubscriptionName == "" {
		return nil, newError(SubscriptionNotFound, "subscription name is required for consumer")
	}

	if options.ReceiverQueueSize <= 0 {
		options.ReceiverQueueSize = defaultReceiverQueueSize
	}

	if options.Interceptors == nil {
		options.Interceptors = defaultConsumerInterceptors
	}

	if options.Name == "" {
		options.Name = generateRandomName()
	}

	if options.Schema != nil && options.Schema.GetSchemaInfo() != nil {
		if options.Schema.GetSchemaInfo().Type == NONE {
			options.Schema = NewBytesSchema(nil)
		}
	}

	// did the user pass in a message channel?
	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 10)
	}

	if options.RetryEnable {
		usingTopic := ""
		if options.Topic != "" {
			usingTopic = options.Topic
		} else if len(options.Topics) > 0 {
			usingTopic = options.Topics[0]
		}
		tn, err := internal.ParseTopicName(usingTopic)
		if err != nil {
			return nil, err
		}

		retryTopic := tn.Domain + "://" + tn.Namespace + "/" + options.SubscriptionName + RetryTopicSuffix
		dlqTopic := tn.Domain + "://" + tn.Namespace + "/" + options.SubscriptionName + DlqTopicSuffix
		if options.DLQ == nil {
			options.DLQ = &DLQPolicy{
				MaxDeliveries:    MaxReconsumeTimes,
				DeadLetterTopic:  dlqTopic,
				RetryLetterTopic: retryTopic,
			}
		} else {
			if options.DLQ.DeadLetterTopic == "" {
				options.DLQ.DeadLetterTopic = dlqTopic
			}
			if options.DLQ.RetryLetterTopic == "" {
				options.DLQ.RetryLetterTopic = retryTopic
			}
		}
		if options.Topic != "" && len(options.Topics) == 0 {
			options.Topics = []string{options.Topic, options.DLQ.RetryLetterTopic}
			options.Topic = ""
		} else if options.Topic == "" && len(options.Topics) > 0 {
			options.Topics = append(options.Topics, options.DLQ.RetryLetterTopic)
		}
	}

	dlq, err := newDlqRouter(client, options.DLQ, client.log)
	if err != nil {
		return nil, err
	}
	rlq, err := newRetryRouter(client, options.DLQ, options.RetryEnable, client.log)
	if err != nil {
		return nil, err
	}

	// normalize as FQDN topics
	var tns []*internal.TopicName
	// single topic consumer
	if options.Topic != "" || len(options.Topics) == 1 {
		topic := options.Topic
		if topic == "" {
			topic = options.Topics[0]
		}

		if tns, err = validateTopicNames(topic); err != nil {
			return nil, err
		}
		topic = tns[0].Name
		return topicSubscribe(client, options, topic, messageCh, dlq, rlq)
	}

	if len(options.Topics) > 1 {
		if tns, err = validateTopicNames(options.Topics...); err != nil {
			return nil, err
		}
		for i := range options.Topics {
			options.Topics[i] = tns[i].Name
		}
		options.Topics = distinct(options.Topics)

		return newMultiTopicConsumer(client, options, options.Topics, messageCh, dlq, rlq)
	}

	if options.TopicsPattern != "" {
		tn, err := internal.ParseTopicName(options.TopicsPattern)
		if err != nil {
			return nil, err
		}

		pattern, err := extractTopicPattern(tn)
		if err != nil {
			return nil, err
		}
		return newRegexConsumer(client, options, tn, pattern, messageCh, dlq, rlq)
	}

	return nil, newError(InvalidTopicName, "topic name is required for consumer")
}

func newInternalConsumer(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage, dlq *dlqRouter, rlq *retryRouter, disableForceTopicCreation bool) (*consumer, error) {

	consumer := &consumer{
		topic:                     topic,
		client:                    client,
		options:                   options,
		disableForceTopicCreation: disableForceTopicCreation,
		messageCh:                 messageCh,
		closeCh:                   make(chan struct{}),
		errorCh:                   make(chan error),
		dlq:                       dlq,
		rlq:                       rlq,
		log:                       client.log.SubLogger(log.Fields{"topic": topic}),
		consumerName:              options.Name,
		metrics:                   client.metrics.GetTopicMetrics(topic),
	}

	err := consumer.internalTopicSubscribeToPartitions()
	if err != nil {
		return nil, err
	}

	// set up timer to monitor for new partitions being added
	duration := options.AutoDiscoveryPeriod
	if duration <= 0 {
		duration = defaultAutoDiscoveryDuration
	}
	consumer.stopDiscovery = consumer.runBackgroundPartitionDiscovery(duration)

	return consumer, nil
}

// Name returns the name of consumer.
func (c *consumer) Name() string {
	return c.consumerName
}

func (c *consumer) runBackgroundPartitionDiscovery(period time.Duration) (cancel func()) {
	var wg sync.WaitGroup
	stopDiscoveryCh := make(chan struct{})
	ticker := time.NewTicker(period)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopDiscoveryCh:
				return
			case <-ticker.C:
				c.log.Debug("Auto discovering new partitions")
				c.internalTopicSubscribeToPartitions()
			}
		}
	}()

	return func() {
		ticker.Stop()
		close(stopDiscoveryCh)
		wg.Wait()
	}
}

func (c *consumer) internalTopicSubscribeToPartitions() error {
	partitions, err := c.client.TopicPartitions(c.topic)
	if err != nil {
		return err
	}

	oldNumPartitions := 0
	newNumPartitions := len(partitions)

	c.Lock()
	defer c.Unlock()
	oldConsumers := c.consumers

	if oldConsumers != nil {
		oldNumPartitions = len(oldConsumers)
		if oldNumPartitions == newNumPartitions {
			c.log.Debug("Number of partitions in topic has not changed")
			return nil
		}

		c.log.WithField("old_partitions", oldNumPartitions).
			WithField("new_partitions", newNumPartitions).
			Info("Changed number of partitions in topic")
	}

	c.consumers = make([]*partitionConsumer, newNumPartitions)

	// Copy over the existing consumer instances
	for i := 0; i < oldNumPartitions; i++ {
		c.consumers[i] = oldConsumers[i]
	}

	type ConsumerError struct {
		err       error
		partition int
		consumer  *partitionConsumer
	}

	receiverQueueSize := c.options.ReceiverQueueSize
	metadata := c.options.Properties

	partitionsToAdd := newNumPartitions - oldNumPartitions
	var wg sync.WaitGroup
	ch := make(chan ConsumerError, partitionsToAdd)
	wg.Add(partitionsToAdd)

	for partitionIdx := oldNumPartitions; partitionIdx < newNumPartitions; partitionIdx++ {
		partitionTopic := partitions[partitionIdx]

		go func(idx int, pt string) {
			defer wg.Done()

			var nackRedeliveryDelay time.Duration
			if c.options.NackRedeliveryDelay == 0 {
				nackRedeliveryDelay = defaultNackRedeliveryDelay
			} else {
				nackRedeliveryDelay = c.options.NackRedeliveryDelay
			}
			opts := &partitionConsumerOpts{
				topic:                      pt,
				consumerName:               c.consumerName,
				subscription:               c.options.SubscriptionName,
				subscriptionType:           c.options.Type,
				subscriptionInitPos:        c.options.SubscriptionInitialPosition,
				partitionIdx:               idx,
				receiverQueueSize:          receiverQueueSize,
				nackRedeliveryDelay:        nackRedeliveryDelay,
				metadata:                   metadata,
				replicateSubscriptionState: c.options.ReplicateSubscriptionState,
				startMessageID:             trackingMessageID{},
				subscriptionMode:           durable,
				readCompacted:              c.options.ReadCompacted,
				interceptors:               c.options.Interceptors,
				maxReconnectToBroker:       c.options.MaxReconnectToBroker,
				keySharedPolicy:            c.options.KeySharedPolicy,
				schema:                     c.options.Schema,
			}
			cons, err := newPartitionConsumer(c, c.client, opts, c.messageCh, c.dlq, c.metrics)
			ch <- ConsumerError{
				err:       err,
				partition: idx,
				consumer:  cons,
			}
		}(partitionIdx, partitionTopic)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for ce := range ch {
		if ce.err != nil {
			err = ce.err
		} else {
			c.consumers[ce.partition] = ce.consumer
		}
	}

	if err != nil {
		// Since there were some failures,
		// cleanup all the partitions that succeeded in creating the consumer
		for _, c := range c.consumers {
			if c != nil {
				c.Close()
			}
		}
		return err
	}

	c.metrics.ConsumersPartitions.Add(float64(partitionsToAdd))
	return nil
}

func topicSubscribe(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage, dlqRouter *dlqRouter, retryRouter *retryRouter) (Consumer, error) {
	c, err := newInternalConsumer(client, options, topic, messageCh, dlqRouter, retryRouter, false)
	if err == nil {
		c.metrics.ConsumersOpened.Inc()
	}
	return c, err
}

func (c *consumer) Subscription() string {
	return c.options.SubscriptionName
}

func (c *consumer) Unsubscribe() error {
	c.Lock()
	defer c.Unlock()

	var errMsg string
	for _, consumer := range c.consumers {
		if err := consumer.Unsubscribe(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", consumer.topic, c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (c *consumer) Receive(ctx context.Context) (message Message, err error) {
	for {
		select {
		case <-c.closeCh:
			return nil, newError(ConsumerClosed, "consumer closed")
		case cm, ok := <-c.messageCh:
			if !ok {
				return nil, newError(ConsumerClosed, "consumer closed")
			}
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Messages
func (c *consumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

// Ack the consumption of a single message
func (c *consumer) Ack(msg Message) {
	c.AckID(msg.ID())
}

// Ack the consumption of a single message, identified by its MessageID
func (c *consumer) AckID(msgID MessageID) {
	mid, ok := c.messageID(msgID)
	if !ok {
		return
	}

	if mid.consumer != nil {
		mid.Ack()
		return
	}

	c.consumers[mid.partitionIdx].AckID(mid)
}

// ReconsumeLater mark a message for redelivery after custom delay
func (c *consumer) ReconsumeLater(msg Message, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	msgID, ok := c.messageID(msg.ID())
	if !ok {
		return
	}
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}

	reconsumeTimes := 1
	if s, ok := props[SysPropertyReconsumeTimes]; ok {
		reconsumeTimes, _ = strconv.Atoi(s)
		reconsumeTimes++
	} else {
		props[SysPropertyRealTopic] = msg.Topic()
		props[SysPropertyOriginMessageID] = msgID.messageID.String()
	}
	props[SysPropertyReconsumeTimes] = strconv.Itoa(reconsumeTimes)
	props[SysPropertyDelayTime] = fmt.Sprintf("%d", int64(delay)/1e6)

	consumerMsg := ConsumerMessage{
		Consumer: c,
		Message: &message{
			payLoad:    msg.Payload(),
			properties: props,
			msgID:      msgID,
		},
	}
	if uint32(reconsumeTimes) > c.dlq.policy.MaxDeliveries {
		c.dlq.Chan() <- consumerMsg
	} else {
		c.rlq.Chan() <- RetryMessage{
			consumerMsg: consumerMsg,
			producerMsg: ProducerMessage{
				Payload:      msg.Payload(),
				Key:          msg.Key(),
				OrderingKey:  msg.OrderingKey(),
				Properties:   props,
				DeliverAfter: delay,
			},
		}
	}
}

func (c *consumer) Nack(msg Message) {
	c.NackID(msg.ID())
}

func (c *consumer) NackID(msgID MessageID) {
	mid, ok := c.messageID(msgID)
	if !ok {
		return
	}

	if mid.consumer != nil {
		mid.Nack()
		return
	}

	c.consumers[mid.partitionIdx].NackID(mid)
}

func (c *consumer) Close() {
	c.closeOnce.Do(func() {
		c.stopDiscovery()

		c.Lock()
		defer c.Unlock()

		var wg sync.WaitGroup
		for i := range c.consumers {
			wg.Add(1)
			go func(pc *partitionConsumer) {
				defer wg.Done()
				pc.Close()
			}(c.consumers[i])
		}
		wg.Wait()
		close(c.closeCh)
		c.client.handlers.Del(c)
		c.dlq.close()
		c.rlq.close()
		c.metrics.ConsumersClosed.Inc()
		c.metrics.ConsumersPartitions.Sub(float64(len(c.consumers)))
	})
}

func (c *consumer) Seek(msgID MessageID) error {
	c.Lock()
	defer c.Unlock()

	if len(c.consumers) > 1 {
		return newError(SeekFailed, "for partition topic, seek command should perform on the individual partitions")
	}

	mid, ok := c.messageID(msgID)
	if !ok {
		return nil
	}

	return c.consumers[mid.partitionIdx].Seek(mid)
}

func (c *consumer) SeekByTime(time time.Time) error {
	c.Lock()
	defer c.Unlock()
	if len(c.consumers) > 1 {
		return newError(SeekFailed, "for partition topic, seek command should perform on the individual partitions")
	}

	return c.consumers[0].SeekByTime(time)
}

var r = &random{
	R: rand.New(rand.NewSource(time.Now().UnixNano())),
}

type random struct {
	sync.Mutex
	R *rand.Rand
}

func generateRandomName() string {
	r.Lock()
	defer r.Unlock()
	chars := "abcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 5)
	for i := range bytes {
		bytes[i] = chars[r.R.Intn(len(chars))]
	}
	return string(bytes)
}

func distinct(fqdnTopics []string) []string {
	set := make(map[string]struct{})
	uniques := make([]string, 0, len(fqdnTopics))
	for _, topic := range fqdnTopics {
		if _, ok := set[topic]; !ok {
			set[topic] = struct{}{}
			uniques = append(uniques, topic)
		}
	}
	return uniques
}

func toProtoSubType(st SubscriptionType) pb.CommandSubscribe_SubType {
	switch st {
	case Exclusive:
		return pb.CommandSubscribe_Exclusive
	case Shared:
		return pb.CommandSubscribe_Shared
	case Failover:
		return pb.CommandSubscribe_Failover
	case KeyShared:
		return pb.CommandSubscribe_Key_Shared
	}

	return pb.CommandSubscribe_Exclusive
}

func toProtoInitialPosition(p SubscriptionInitialPosition) pb.CommandSubscribe_InitialPosition {
	switch p {
	case SubscriptionPositionLatest:
		return pb.CommandSubscribe_Latest
	case SubscriptionPositionEarliest:
		return pb.CommandSubscribe_Earliest
	}

	return pb.CommandSubscribe_Latest
}

func (c *consumer) messageID(msgID MessageID) (trackingMessageID, bool) {
	mid, ok := toTrackingMessageID(msgID)
	if !ok {
		c.log.Warnf("invalid message id type %T", msgID)
		return trackingMessageID{}, false
	}

	partition := int(mid.partitionIdx)
	// did we receive a valid partition index?
	if partition < 0 || partition >= len(c.consumers) {
		c.log.Warnf("invalid partition index %d expected a partition between [0-%d]",
			partition, len(c.consumers))
		return trackingMessageID{}, false
	}

	return mid, true
}

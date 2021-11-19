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
	pkgerrors "github.com/pkg/errors"

	"github.com/apache/pulsar-client-go/pulsar/log"
)

type multiTopicConsumer struct {
	client *client

	options ConsumerOptions

	consumerName string
	messageCh    chan ConsumerMessage

	consumers map[string]Consumer

	dlq       *dlqRouter
	rlq       *retryRouter
	closeOnce sync.Once
	closeCh   chan struct{}

	log log.Logger
}

func newMultiTopicConsumer(client *client, options ConsumerOptions, topics []string,
	messageCh chan ConsumerMessage, dlq *dlqRouter, rlq *retryRouter) (Consumer, error) {
	mtc := &multiTopicConsumer{
		client:       client,
		options:      options,
		messageCh:    messageCh,
		consumers:    make(map[string]Consumer, len(topics)),
		closeCh:      make(chan struct{}),
		dlq:          dlq,
		rlq:          rlq,
		log:          client.log.SubLogger(log.Fields{"topic": topics}),
		consumerName: options.Name,
	}

	var errs error
	for ce := range subscriber(client, topics, options, messageCh, dlq, rlq) {
		if ce.err != nil {
			errs = pkgerrors.Wrapf(ce.err, "unable to subscribe to topic=%s", ce.topic)
		} else {
			mtc.consumers[ce.topic] = ce.consumer
		}
	}

	if errs != nil {
		for _, c := range mtc.consumers {
			c.Close()
		}
		return nil, errs
	}

	return mtc, nil
}

func (c *multiTopicConsumer) Subscription() string {
	return c.options.SubscriptionName
}

func (c *multiTopicConsumer) Unsubscribe() error {
	var errs error
	for t, consumer := range c.consumers {
		if err := consumer.Unsubscribe(); err != nil {
			msg := fmt.Sprintf("unable to unsubscribe from topic=%s subscription=%s",
				t, c.Subscription())
			errs = pkgerrors.Wrap(err, msg)
		}
	}
	return errs
}

func (c *multiTopicConsumer) Receive(ctx context.Context) (message Message, err error) {
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
func (c *multiTopicConsumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

// Ack the consumption of a single message
func (c *multiTopicConsumer) Ack(msg Message) {
	c.AckID(msg.ID())
}

// Ack the consumption of a single message, identified by its MessageID
func (c *multiTopicConsumer) AckID(msgID MessageID) {
	mid, ok := toTrackingMessageID(msgID)
	if !ok {
		c.log.Warnf("invalid message id type %T", msgID)
		return
	}

	if mid.consumer == nil {
		c.log.Warnf("unable to ack messageID=%+v can not determine topic", msgID)
		return
	}

	mid.Ack()
}

func (c *multiTopicConsumer) ReconsumeLater(msg Message, delay time.Duration) {
	names, err := validateTopicNames(msg.Topic())
	if err != nil {
		c.log.Errorf("validate msg topic %q failed: %v", msg.Topic(), err)
		return
	}
	if len(names) != 1 {
		c.log.Errorf("invalid msg topic %q names: %+v ", msg.Topic(), names)
		return
	}

	tn := names[0]
	fqdnTopic := internal.TopicNameWithoutPartitionPart(tn)
	consumer, ok := c.consumers[fqdnTopic]
	if !ok {
		// check to see if the topic with the partition part is in the consumers
		// this can happen when the consumer is configured to consume from a specific partition
		if consumer, ok = c.consumers[tn.Name]; !ok {
			c.log.Warnf("consumer of topic %s not exist unexpectedly", msg.Topic())
			return
		}
	}
	consumer.ReconsumeLater(msg, delay)
}

func (c *multiTopicConsumer) Nack(msg Message) {
	c.NackID(msg.ID())
}

func (c *multiTopicConsumer) NackID(msgID MessageID) {
	mid, ok := toTrackingMessageID(msgID)
	if !ok {
		c.log.Warnf("invalid message id type %T", msgID)
		return
	}

	if mid.consumer == nil {
		c.log.Warnf("unable to nack messageID=%+v can not determine topic", msgID)
		return
	}

	mid.Nack()
}

func (c *multiTopicConsumer) Close() {
	c.closeOnce.Do(func() {
		var wg sync.WaitGroup
		wg.Add(len(c.consumers))
		for _, con := range c.consumers {
			go func(consumer Consumer) {
				defer wg.Done()
				consumer.Close()
			}(con)
		}
		wg.Wait()
		close(c.closeCh)
		c.client.handlers.Del(c)
		c.dlq.close()
		c.rlq.close()
	})
}

func (c *multiTopicConsumer) Seek(msgID MessageID) error {
	return newError(SeekFailed, "seek command not allowed for multi topic consumer")
}

func (c *multiTopicConsumer) SeekByTime(time time.Time) error {
	return newError(SeekFailed, "seek command not allowed for multi topic consumer")
}

// Name returns the name of consumer.
func (c *multiTopicConsumer) Name() string {
	return c.consumerName
}

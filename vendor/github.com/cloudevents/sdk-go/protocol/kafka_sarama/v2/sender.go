/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_sarama

import (
	"context"

	"github.com/Shopify/sarama"

	"github.com/cloudevents/sdk-go/v2/binding"
)

// Sender implements binding.Sender that sends messages to a specific receiverTopic using sarama.SyncProducer
type Sender struct {
	topic        string
	syncProducer sarama.SyncProducer
}

// NewSender returns a binding.Sender that sends messages to a specific receiverTopic using sarama.SyncProducer
func NewSender(brokers []string, saramaConfig *sarama.Config, topic string, options ...SenderOptionFunc) (*Sender, error) {
	// Force this setting because it's required by sarama SyncProducer
	saramaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return makeSender(producer, topic, options...), nil
}

// NewSenderFromClient returns a binding.Sender that sends messages to a specific receiverTopic using sarama.SyncProducer
func NewSenderFromClient(client sarama.Client, topic string, options ...SenderOptionFunc) (*Sender, error) {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return makeSender(producer, topic, options...), nil
}

// NewSenderFromSyncProducer returns a binding.Sender that sends messages to a specific topic using sarama.SyncProducer
func NewSenderFromSyncProducer(topic string, syncProducer sarama.SyncProducer, options ...SenderOptionFunc) (*Sender, error) {
	return makeSender(syncProducer, topic, options...), nil
}

func makeSender(syncProducer sarama.SyncProducer, topic string, options ...SenderOptionFunc) *Sender {
	s := &Sender{
		topic:        topic,
		syncProducer: syncProducer,
	}
	for _, o := range options {
		o(s)
	}
	return s
}

func (s *Sender) Send(ctx context.Context, m binding.Message, transformers ...binding.Transformer) error {
	var err error
	defer m.Finish(err)

	kafkaMessage := sarama.ProducerMessage{Topic: s.topic}

	if k := ctx.Value(withMessageKey{}); k != nil {
		kafkaMessage.Key = k.(sarama.Encoder)
	}

	if err = WriteProducerMessage(ctx, m, &kafkaMessage, transformers...); err != nil {
		return err
	}

	_, _, err = s.syncProducer.SendMessage(&kafkaMessage)
	// Somebody closed the client while sending the message, so no problem here
	if err == sarama.ErrClosedClient {
		return nil
	}
	return err
}

func (s *Sender) Close(ctx context.Context) error {
	// If the Sender was built with NewSenderFromClient, this Close will close only the producer,
	// otherwise it will close the whole client
	return s.syncProducer.Close()
}

type withMessageKey struct{}

// WithMessageKey allows to set the key used when sending the producer message
func WithMessageKey(ctx context.Context, key sarama.Encoder) context.Context {
	return context.WithValue(ctx, withMessageKey{}, key)
}

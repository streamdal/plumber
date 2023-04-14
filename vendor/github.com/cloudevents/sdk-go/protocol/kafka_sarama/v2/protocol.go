/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_sarama

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"

	"github.com/cloudevents/sdk-go/v2/binding"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/cloudevents/sdk-go/v2/protocol"
)

const (
	defaultGroupId = "cloudevents-sdk-go"
)

type Protocol struct {
	// Kafka
	Client     sarama.Client
	ownsClient bool

	// Sender
	Sender *Sender

	// Sender options
	SenderContextDecorators []func(context.Context) context.Context
	senderTopic             string

	// Consumer
	Consumer    *Consumer
	consumerMux sync.Mutex

	// Consumer options
	receiverTopic   string
	receiverGroupId string
}

// NewProtocol creates a new kafka transport.
func NewProtocol(brokers []string, saramaConfig *sarama.Config, sendToTopic string, receiveFromTopic string, opts ...ProtocolOptionFunc) (*Protocol, error) {
	// Force this setting because it's required by sarama SyncProducer
	saramaConfig.Producer.Return.Successes = true
	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	p, err := NewProtocolFromClient(client, sendToTopic, receiveFromTopic, opts...)
	if err != nil {
		return nil, err
	}
	p.ownsClient = true
	return p, nil
}

// NewProtocolFromClient creates a new kafka transport starting from a sarama.Client
func NewProtocolFromClient(client sarama.Client, sendToTopic string, receiveFromTopic string, opts ...ProtocolOptionFunc) (*Protocol, error) {
	p := &Protocol{
		Client:                  client,
		SenderContextDecorators: make([]func(context.Context) context.Context, 0),
		receiverGroupId:         defaultGroupId,
		senderTopic:             sendToTopic,
		receiverTopic:           receiveFromTopic,
		ownsClient:              false,
	}

	var err error
	if err = p.applyOptions(opts...); err != nil {
		return nil, err
	}

	if p.senderTopic == "" {
		return nil, errors.New("you didn't specify the topic to send to")
	}
	p.Sender, err = NewSenderFromClient(p.Client, p.senderTopic)
	if err != nil {
		return nil, err
	}

	if p.receiverTopic == "" {
		return nil, errors.New("you didn't specify the topic to receive from")
	}
	p.Consumer = NewConsumerFromClient(p.Client, p.receiverGroupId, p.receiverTopic)

	return p, nil
}

func (p *Protocol) applyOptions(opts ...ProtocolOptionFunc) error {
	for _, fn := range opts {
		fn(p)
	}
	return nil
}

// OpenInbound implements Opener.OpenInbound
// NOTE: This is a blocking call.
func (p *Protocol) OpenInbound(ctx context.Context) error {
	p.consumerMux.Lock()
	defer p.consumerMux.Unlock()

	logger := cecontext.LoggerFrom(ctx)
	logger.Infof("Starting consumer group to topic %s and group id %s", p.receiverTopic, p.receiverGroupId)

	return p.Consumer.OpenInbound(ctx)
}

func (p *Protocol) Send(ctx context.Context, in binding.Message, transformers ...binding.Transformer) error {
	for _, f := range p.SenderContextDecorators {
		ctx = f(ctx)
	}
	return p.Sender.Send(ctx, in, transformers...)
}

func (p *Protocol) Receive(ctx context.Context) (binding.Message, error) {
	return p.Consumer.Receive(ctx)
}

func (p *Protocol) Close(ctx context.Context) error {
	if p.ownsClient {
		// Just closing the client here closes at cascade consumer and producer
		return p.Client.Close()
	}
	if err := p.Consumer.Close(ctx); err != nil {
		return err
	}
	return p.Sender.Close(ctx)
}

// Kafka protocol implements Sender, Receiver
var _ protocol.Sender = (*Protocol)(nil)
var _ protocol.Receiver = (*Protocol)(nil)
var _ protocol.Closer = (*Protocol)(nil)

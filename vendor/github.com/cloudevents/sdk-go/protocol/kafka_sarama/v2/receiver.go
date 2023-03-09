/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_sarama

import (
	"context"
	"io"
	"sync"

	"github.com/Shopify/sarama"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
)

type msgErr struct {
	msg binding.Message
	err error
}

// Receiver which implements sarama.ConsumerGroupHandler
// After the first invocation of Receiver.Receive(), the sarama.ConsumerGroup is created and started.
type Receiver struct {
	once     sync.Once
	incoming chan msgErr
}

// NewReceiver creates a Receiver which implements sarama.ConsumerGroupHandler
// The sarama.ConsumerGroup must be started invoking. If you need a Receiver which also manage the ConsumerGroup, use NewConsumer
// After the first invocation of Receiver.Receive(), the sarama.ConsumerGroup is created and started.
func NewReceiver() *Receiver {
	return &Receiver{
		incoming: make(chan msgErr),
	}
}

func (r *Receiver) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (r *Receiver) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (r *Receiver) Close(context.Context) error {
	r.once.Do(func() {
		close(r.incoming)
	})
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Also the method should return when `session.Context()` is done.
// Refer - https://github.com/Shopify/sarama/blob/5e2c2ef0e429f895c86152189f625bfdad7d3452/examples/consumergroup/main.go#L177
func (r *Receiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case msg := <-claim.Messages():
			m := NewMessageFromConsumerMessage(msg)

			r.incoming <- msgErr{
				msg: binding.WithFinish(m, func(err error) {
					if protocol.IsACK(err) {
						session.MarkMessage(msg, "")
					}
				}),
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		// https://github.com/Shopify/sarama/issues/2118
		// Also checked Shopify/sarama code which calls this ConsumeClaim method, and don't see if there is any difference
		// whether this method returns error or not. If it returns the error, as per current implementation, it could
		// get printed in logs and later drained when the ConsumerGroup gets closed.
		// For now, to be on safer side, returning nil instead of session.Context().Err() as suggested in
		// https://github.com/Shopify/sarama/blob/5e2c2ef0e429f895c86152189f625bfdad7d3452/examples/consumergroup/main.go
		case <-session.Context().Done():
			return nil
		}
	}
}

func (r *Receiver) Receive(ctx context.Context) (binding.Message, error) {
	select {
	case <-ctx.Done():
		return nil, io.EOF
	case msgErr, ok := <-r.incoming:
		if !ok {
			return nil, io.EOF
		}
		return msgErr.msg, msgErr.err
	}
}

var _ protocol.Receiver = (*Receiver)(nil)
var _ protocol.Closer = (*Receiver)(nil)

type Consumer struct {
	Receiver

	client    sarama.Client
	ownClient bool

	topic   string
	groupId string

	cgMtx sync.Mutex
}

func NewConsumer(brokers []string, saramaConfig *sarama.Config, groupId string, topic string) (*Consumer, error) {
	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	consumer := NewConsumerFromClient(client, groupId, topic)
	consumer.ownClient = true

	return consumer, nil
}

func NewConsumerFromClient(client sarama.Client, groupId string, topic string) *Consumer {
	return &Consumer{
		Receiver: Receiver{
			incoming: make(chan msgErr),
		},
		client:    client,
		topic:     topic,
		groupId:   groupId,
		ownClient: false,
	}
}

func (c *Consumer) OpenInbound(ctx context.Context) error {
	c.cgMtx.Lock()
	defer c.cgMtx.Unlock()
	cg, err := sarama.NewConsumerGroupFromClient(c.groupId, c.client)
	if err != nil {
		return err
	}

	errCh := make(chan error)

	go c.startConsumerGroupLoop(cg, ctx, errCh)

	select {
	case <-ctx.Done():
		return cg.Close()
	case err = <-errCh:
		// We still need to close this thing
		err2 := cg.Close()
		if err == nil {
			err = err2
		}
		// Somebody else closed the client, so no problem here
		if err == sarama.ErrClosedClient || err == sarama.ErrClosedConsumerGroup {
			return nil
		}
		return err
	}
}

func (c *Consumer) startConsumerGroupLoop(cg sarama.ConsumerGroup, ctx context.Context, errs chan<- error) {
	defer c.Receiver.Close(ctx)
	// Need to be wrapped in a for loop
	// https://godoc.org/github.com/Shopify/sarama#ConsumerGroup
	for {
		err := cg.Consume(context.Background(), []string{c.topic}, c)

		select {
		// If context is closed, then consumer group session was closed by the user
		case <-ctx.Done():
			if err != nil {
				errs <- err
			}
			return
		// Something else happened
		default:
			if err == nil {
				continue
			} else if err == sarama.ErrClosedClient || err == sarama.ErrClosedConsumerGroup {
				// Consumer group closed correctly, we can close that loop
				return
			} else {
				// Another error happened (eg a disconnection to the cluster)
				// We need to loop again
				errs <- err
			}
		}
	}
}

func (c *Consumer) Close(ctx context.Context) error {
	if c.ownClient {
		return c.client.Close()
	}
	return nil
}

var _ protocol.Opener = (*Consumer)(nil)
var _ protocol.Closer = (*Consumer)(nil)

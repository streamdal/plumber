/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package nats

import (
	"github.com/nats-io/nats.go"
)

// The Subscriber interface allows us to configure how the subscription is created
type Subscriber interface {
	Subscribe(conn *nats.Conn, subject string, cb nats.MsgHandler) (*nats.Subscription, error)
}

// RegularSubscriber creates regular subscriptions
type RegularSubscriber struct {
}

// Subscribe implements Subscriber.Subscribe
func (s *RegularSubscriber) Subscribe(conn *nats.Conn, subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return conn.Subscribe(subject, cb)
}

var _ Subscriber = (*RegularSubscriber)(nil)

// QueueSubscriber creates queue subscriptions
type QueueSubscriber struct {
	Queue string
}

// Subscribe implements Subscriber.Subscribe
func (s *QueueSubscriber) Subscribe(conn *nats.Conn, subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return conn.QueueSubscribe(subject, s.Queue, cb)
}

var _ Subscriber = (*QueueSubscriber)(nil)

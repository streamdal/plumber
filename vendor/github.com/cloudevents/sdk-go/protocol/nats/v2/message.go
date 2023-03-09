/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package nats

import (
	"bytes"
	"context"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/nats-io/nats.go"
)

// Message implements binding.Message by wrapping an *nats.Msg.
// This message *can* be read several times safely
type Message struct {
	Msg      *nats.Msg
	encoding binding.Encoding
}

// NewMessage wraps an *nats.Msg in a binding.Message.
// The returned message *can* be read several times safely
func NewMessage(msg *nats.Msg) *Message {
	return &Message{Msg: msg, encoding: binding.EncodingStructured}
}

var _ binding.Message = (*Message)(nil)

func (m *Message) ReadEncoding() binding.Encoding {
	return m.encoding
}

func (m *Message) ReadStructured(ctx context.Context, encoder binding.StructuredWriter) error {
	return encoder.SetStructuredEvent(ctx, format.JSON, bytes.NewReader(m.Msg.Data))
}

func (m *Message) ReadBinary(ctx context.Context, encoder binding.BinaryWriter) error {
	return binding.ErrNotBinary
}

func (m *Message) Finish(err error) error {
	return nil
}

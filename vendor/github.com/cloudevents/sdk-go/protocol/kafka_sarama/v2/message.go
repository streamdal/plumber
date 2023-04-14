/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_sarama

import (
	"bytes"
	"context"
	"strings"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/binding/spec"

	"github.com/Shopify/sarama"
)

const (
	prefix            = "ce_"
	contentTypeHeader = "content-type"
)

var specs = spec.WithPrefix(prefix)

// Message holds a Kafka Message.
// This message *can* be read several times safely
type Message struct {
	Value       []byte
	Headers     map[string][]byte
	ContentType string
	format      format.Format
	version     spec.Version
}

// Check if http.Message implements binding.Message
var _ binding.Message = (*Message)(nil)
var _ binding.MessageMetadataReader = (*Message)(nil)

// NewMessageFromConsumerMessage returns a binding.Message that holds the provided ConsumerMessage.
// The returned binding.Message *can* be read several times safely
// This function *doesn't* guarantee that the returned binding.Message is always a kafka_sarama.Message instance
func NewMessageFromConsumerMessage(cm *sarama.ConsumerMessage) *Message {
	var contentType string
	headers := make(map[string][]byte, len(cm.Headers))
	for _, r := range cm.Headers {
		k := strings.ToLower(string(r.Key))
		if k == contentTypeHeader {
			contentType = string(r.Value)
		}
		headers[k] = r.Value
	}
	return NewMessage(cm.Value, contentType, headers)
}

// NewMessage returns a binding.Message that holds the provided kafka message components.
// The returned binding.Message *can* be read several times safely
// This function *doesn't* guarantee that the returned binding.Message is always a kafka_sarama.Message instance
func NewMessage(value []byte, contentType string, headers map[string][]byte) *Message {
	if ft := format.Lookup(contentType); ft != nil {
		return &Message{
			Value:       value,
			ContentType: contentType,
			Headers:     headers,
			format:      ft,
		}
	} else if v := specs.Version(string(headers[specs.PrefixedSpecVersionName()])); v != nil {
		return &Message{
			Value:       value,
			ContentType: contentType,
			Headers:     headers,
			version:     v,
		}
	}

	return &Message{
		Value:       value,
		ContentType: contentType,
		Headers:     headers,
	}
}

func (m *Message) ReadEncoding() binding.Encoding {
	if m.version != nil {
		return binding.EncodingBinary
	}
	if m.format != nil {
		return binding.EncodingStructured
	}
	return binding.EncodingUnknown
}

func (m *Message) ReadStructured(ctx context.Context, encoder binding.StructuredWriter) error {
	if m.format != nil {
		return encoder.SetStructuredEvent(ctx, m.format, bytes.NewReader(m.Value))
	}
	return binding.ErrNotStructured
}

func (m *Message) ReadBinary(ctx context.Context, encoder binding.BinaryWriter) (err error) {
	if m.version == nil {
		return binding.ErrNotBinary
	}

	for k, v := range m.Headers {
		if strings.HasPrefix(k, prefix) {
			attr := m.version.Attribute(k)
			if attr != nil {
				err = encoder.SetAttribute(attr, string(v))
			} else {
				err = encoder.SetExtension(strings.TrimPrefix(k, prefix), string(v))
			}
		} else if k == contentTypeHeader {
			err = encoder.SetAttribute(m.version.AttributeFromKind(spec.DataContentType), string(v))
		}
		if err != nil {
			return
		}
	}

	if m.Value != nil {
		err = encoder.SetData(bytes.NewBuffer(m.Value))
	}

	return
}

func (m *Message) GetAttribute(k spec.Kind) (spec.Attribute, interface{}) {
	attr := m.version.AttributeFromKind(k)
	if attr != nil {
		return attr, string(m.Headers[attr.PrefixedName()])
	}
	return nil, nil
}

func (m *Message) GetExtension(name string) interface{} {
	return string(m.Headers[prefix+name])
}

func (m *Message) Finish(error) error {
	return nil
}

/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_sarama

import (
	"bytes"
	"context"
	"io"

	"github.com/Shopify/sarama"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
	"github.com/cloudevents/sdk-go/v2/types"
)

const (
	partitionKey = "partitionkey"
)

// WriteProducerMessage fills the provided producerMessage with the message m.
// Using context you can tweak the encoding processing (more details on binding.Write documentation).
// By default, this function implements the key mapping, trying to set the key of the message based on partitionKey extension.
// If you want to disable the Key Mapping, decorate the context with `WithSkipKeyMapping`
func WriteProducerMessage(ctx context.Context, m binding.Message, producerMessage *sarama.ProducerMessage, transformers ...binding.Transformer) error {
	writer := (*kafkaProducerMessageWriter)(producerMessage)

	skipKey := binding.GetOrDefaultFromCtx(ctx, skipKeyKey{}, false).(bool)

	var key string

	// If skipKey = false, then we add a transformer that extracts the key
	if !skipKey {
		transformers = append(transformers, binding.TransformerFunc(func(r binding.MessageMetadataReader, w binding.MessageMetadataWriter) error {
			ext := r.GetExtension(partitionKey)
			if !types.IsZero(ext) {
				extStr, err := types.Format(ext)
				if err != nil {
					return err
				}
				key = extStr
			}
			return nil
		}))
	}

	_, err := binding.Write(
		ctx,
		m,
		writer,
		writer,
		transformers...,
	)
	if key != "" {
		producerMessage.Key = sarama.StringEncoder(key)
	}
	return err
}

type kafkaProducerMessageWriter sarama.ProducerMessage

func (b *kafkaProducerMessageWriter) SetStructuredEvent(ctx context.Context, format format.Format, event io.Reader) error {
	b.Headers = []sarama.RecordHeader{{
		Key:   []byte(contentTypeHeader),
		Value: []byte(format.MediaType()),
	}}

	var buf bytes.Buffer
	_, err := io.Copy(&buf, event)
	if err != nil {
		return err
	}

	b.Value = sarama.ByteEncoder(buf.Bytes())
	return nil
}

func (b *kafkaProducerMessageWriter) Start(ctx context.Context) error {
	b.Headers = []sarama.RecordHeader{}
	return nil
}

func (b *kafkaProducerMessageWriter) End(ctx context.Context) error {
	return nil
}

func (b *kafkaProducerMessageWriter) SetData(reader io.Reader) error {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return err
	}

	b.Value = sarama.ByteEncoder(buf.Bytes())
	return nil
}

func (b *kafkaProducerMessageWriter) SetAttribute(attribute spec.Attribute, value interface{}) error {
	if attribute.Kind() == spec.DataContentType {
		if value == nil {
			b.removeHeader(contentTypeHeader)
			return nil
		}

		// Everything is a string here
		s, err := types.Format(value)
		if err != nil {
			return err
		}
		b.Headers = append(b.Headers, sarama.RecordHeader{Key: []byte(contentTypeHeader), Value: []byte(s)})
	} else {
		if value == nil {
			b.removeHeader(prefix + attribute.Name())
			return nil
		}

		// Everything is a string here
		s, err := types.Format(value)
		if err != nil {
			return err
		}
		b.Headers = append(b.Headers, sarama.RecordHeader{Key: []byte(prefix + attribute.Name()), Value: []byte(s)})
	}
	return nil
}

func (b *kafkaProducerMessageWriter) SetExtension(name string, value interface{}) error {
	if value == nil {
		b.removeHeader(prefix + name)
		return nil
	}

	// Kafka headers, everything is a string!
	s, err := types.Format(value)
	if err != nil {
		return err
	}
	b.Headers = append(b.Headers, sarama.RecordHeader{Key: []byte(prefix + name), Value: []byte(s)})
	return nil
}

func (b *kafkaProducerMessageWriter) removeHeader(name string) {
	k := []byte(name)
	for index, h := range b.Headers {
		if bytes.Equal(k, h.Key) {
			b.Headers = append(b.Headers[:index], b.Headers[index+1:]...)
			return
		}
	}
}

type skipKeyKey struct{}

func WithSkipKeyMapping(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipKeyKey{}, true)
}

var _ binding.StructuredWriter = (*kafkaProducerMessageWriter)(nil) // Test it conforms to the interface
var _ binding.BinaryWriter = (*kafkaProducerMessageWriter)(nil)     // Test it conforms to the interface

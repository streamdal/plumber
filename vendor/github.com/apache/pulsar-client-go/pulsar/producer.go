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
	"time"
)

type HashingScheme int

const (
	// JavaStringHash and Java String.hashCode() equivalent
	JavaStringHash HashingScheme = iota
	// Murmur3_32Hash use Murmur3 hashing function
	Murmur3_32Hash
)

type CompressionType int

const (
	NoCompression CompressionType = iota
	LZ4
	ZLib
	ZSTD
)

type CompressionLevel int

const (
	// Default compression level
	Default CompressionLevel = iota

	// Faster compression, with lower compression ration
	Faster

	// Higher compression rate, but slower
	Better
)

// TopicMetadata is a interface of topic metadata
type TopicMetadata interface {
	// NumPartitions get the number of partitions for the specific topic
	NumPartitions() uint32
}

type ProducerOptions struct {
	// Topic specify the topic this producer will be publishing on.
	// This argument is required when constructing the producer.
	Topic string

	// Name specify a name for the producer
	// If not assigned, the system will generate a globally unique name which can be access with
	// Producer.ProducerName().
	// When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
	// across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
	// a topic.
	Name string

	// Properties attach a set of application defined properties to the producer
	// This properties will be visible in the topic stats
	Properties map[string]string

	// SendTimeout set the timeout for a message that not be acknowledged by server since sent.
	// Send and SendAsync returns an error after timeout.
	// Default is 30 seconds, negative such as -1 to disable.
	SendTimeout time.Duration

	// DisableBlockIfQueueFull control whether Send and SendAsync block if producer's message queue is full.
	// Default is false, if set to true then Send and SendAsync return error when queue is full.
	DisableBlockIfQueueFull bool

	// MaxPendingMessages set the max size of the queue holding the messages pending to receive an
	// acknowledgment from the broker.
	MaxPendingMessages int

	// HashingScheme change the `HashingScheme` used to chose the partition on where to publish a particular message.
	// Standard hashing functions available are:
	//
	//  - `JavaStringHash` : Java String.hashCode() equivalent
	//  - `Murmur3_32Hash` : Use Murmur3 hashing function.
	// 		https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash
	//
	// Default is `JavaStringHash`.
	HashingScheme

	// CompressionType set the compression type for the producer.
	// By default, message payloads are not compressed. Supported compression types are:
	//  - LZ4
	//  - ZLIB
	//  - ZSTD
	//
	// Note: ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that
	// release in order to be able to receive messages compressed with ZSTD.
	CompressionType

	// Define the desired compression level. Options:
	// - Default
	// - Faster
	// - Better
	CompressionLevel

	// MessageRouter set a custom message routing policy by passing an implementation of MessageRouter
	// The router is a function that given a particular message and the topic metadata, returns the
	// partition index where the message should be routed to
	MessageRouter func(*ProducerMessage, TopicMetadata) int

	// DisableBatching control whether automatic batching of messages is enabled for the producer. By default batching
	// is enabled.
	// When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
	// broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
	// messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
	// contents.
	// When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
	// Setting `DisableBatching: true` will make the producer to send messages individually
	DisableBatching bool

	// BatchingMaxPublishDelay set the time period within which the messages sent will be batched (default: 10ms)
	// if batch messages are enabled. If set to a non zero value, messages will be queued until this time
	// interval or until
	BatchingMaxPublishDelay time.Duration

	// BatchingMaxMessages set the maximum number of messages permitted in a batch. (default: 1000)
	// If set to a value greater than 1, messages will be queued until this threshold is reached or
	// BatchingMaxSize (see below) has been reached or the batch interval has elapsed.
	BatchingMaxMessages uint

	// BatchingMaxSize sets the maximum number of bytes permitted in a batch. (default 128 KB)
	// If set to a value greater than 1, messages will be queued until this threshold is reached or
	// BatchingMaxMessages (see above) has been reached or the batch interval has elapsed.
	BatchingMaxSize uint

	// A chain of interceptors, These interceptors will be called at some points defined in ProducerInterceptor interface
	Interceptors ProducerInterceptors

	Schema Schema

	// MaxReconnectToBroker set the maximum retry number of reconnectToBroker. (default: ultimate)
	MaxReconnectToBroker *uint

	// BatcherBuilderType sets the batch builder type (default DefaultBatchBuilder)
	// This will be used to create batch container when batching is enabled.
	// Options:
	// - DefaultBatchBuilder
	// - KeyBasedBatchBuilder
	BatcherBuilderType

	// PartitionsAutoDiscoveryInterval is the time interval for the background process to discover new partitions
	// Default is 1 minute
	PartitionsAutoDiscoveryInterval time.Duration
}

// Producer is used to publish messages on a topic
type Producer interface {
	// Topic return the topic to which producer is publishing to
	Topic() string

	// Name return the producer name which could have been assigned by the system or specified by the client
	Name() string

	// Send a message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// producer.Send(ctx, pulsar.ProducerMessage{ Payload: myPayload })
	Send(context.Context, *ProducerMessage) (MessageID, error)

	// SendAsync a message in asynchronous mode
	// This call is blocked when the `event channel` becomes full (default: 10) or the
	// `maxPendingMessages` becomes full (default: 1000)
	// The callback will report back the message being published and
	// the eventual error in publishing
	SendAsync(context.Context, *ProducerMessage, func(MessageID, *ProducerMessage, error))

	// LastSequenceID get the last sequence id that was published by this producer.
	// This represent either the automatically assigned or custom sequence id (set on the ProducerMessage) that
	// was published and acknowledged by the broker.
	// After recreating a producer with the same producer name, this will return the last message that was
	// published in the previous producer session, or -1 if there no message was ever published.
	// return the last sequence id published by this producer.
	LastSequenceID() int64

	// Flush all the messages buffered in the client and wait until all messages have been successfully
	// persisted.
	Flush() error

	// Close the producer and releases resources allocated
	// No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
	// of errors, pending writes will not be retried.
	Close()
}

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

package internal

import (
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

type BuffersPool interface {
	GetBuffer() Buffer
}

// BatcherBuilderProvider defines func which returns the BatchBuilder.
type BatcherBuilderProvider func(
	maxMessages uint, maxBatchSize uint, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, logger log.Logger,
) (BatchBuilder, error)

// BatchBuilder is a interface of batch builders
type BatchBuilder interface {
	// IsFull check if the size in the current batch exceeds the maximum size allowed by the batch
	IsFull() bool

	// Add will add single message to batch.
	Add(
		metadata *pb.SingleMessageMetadata, sequenceIDGenerator *uint64,
		payload []byte,
		callback interface{}, replicateTo []string, deliverAt time.Time,
	) bool

	// Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
	Flush() (batchData Buffer, sequenceID uint64, callbacks []interface{})

	// Flush all the messages buffered in multiple batches and wait until all
	// messages have been successfully persisted.
	FlushBatches() (
		batchData []Buffer, sequenceID []uint64, callbacks [][]interface{},
	)

	// Return the batch container batch message in multiple batches.
	IsMultiBatches() bool

	reset()
	Close() error
}

// batchContainer wraps the objects needed to a batch.
// batchContainer implement BatchBuilder as a single batch container.
type batchContainer struct {
	buffer Buffer

	// Current number of messages in the batch
	numMessages uint

	// Max number of message allowed in the batch
	maxMessages uint

	// The largest size for a batch sent from this particular producer.
	// This is used as a baseline to allocate a new buffer that can hold the entire batch
	// without needing costly re-allocations.
	maxBatchSize uint

	producerName string
	producerID   uint64

	cmdSend     *pb.BaseCommand
	msgMetadata *pb.MessageMetadata
	callbacks   []interface{}

	compressionProvider compression.Provider
	buffersPool         BuffersPool

	log log.Logger
}

// newBatchContainer init a batchContainer
func newBatchContainer(
	maxMessages uint, maxBatchSize uint, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, logger log.Logger,
) batchContainer {

	bc := batchContainer{
		buffer:       NewBuffer(4096),
		numMessages:  0,
		maxMessages:  maxMessages,
		maxBatchSize: maxBatchSize,
		producerName: producerName,
		producerID:   producerID,
		cmdSend: baseCommand(
			pb.BaseCommand_SEND,
			&pb.CommandSend{
				ProducerId: &producerID,
			},
		),
		msgMetadata: &pb.MessageMetadata{
			ProducerName: &producerName,
		},
		callbacks:           []interface{}{},
		compressionProvider: getCompressionProvider(compressionType, level),
		buffersPool:         bufferPool,
		log:                 logger,
	}

	if compressionType != pb.CompressionType_NONE {
		bc.msgMetadata.Compression = &compressionType
	}

	return bc
}

// NewBatchBuilder init batch builder and return BatchBuilder pointer. Build a new batch message container.
func NewBatchBuilder(
	maxMessages uint, maxBatchSize uint, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, logger log.Logger,
) (BatchBuilder, error) {

	bc := newBatchContainer(
		maxMessages, maxBatchSize, producerName, producerID, compressionType,
		level, bufferPool, logger,
	)

	return &bc, nil
}

// IsFull check if the size in the current batch exceeds the maximum size allowed by the batch
func (bc *batchContainer) IsFull() bool {
	return bc.numMessages >= bc.maxMessages || bc.buffer.ReadableBytes() > uint32(bc.maxBatchSize)
}

func (bc *batchContainer) hasSpace(payload []byte) bool {
	msgSize := uint32(len(payload))
	return bc.numMessages > 0 && (bc.buffer.ReadableBytes()+msgSize) > uint32(bc.maxBatchSize)
}

// Add will add single message to batch.
func (bc *batchContainer) Add(
	metadata *pb.SingleMessageMetadata, sequenceIDGenerator *uint64,
	payload []byte,
	callback interface{}, replicateTo []string, deliverAt time.Time,
) bool {
	if replicateTo != nil && bc.numMessages != 0 {
		// If the current batch is not empty and we're trying to set the replication clusters,
		// then we need to force the current batch to flush and send the message individually
		return false
	} else if bc.msgMetadata.ReplicateTo != nil {
		// There's already a message with cluster replication list. need to flush before next
		// message can be sent
		return false
	} else if bc.hasSpace(payload) {
		// The current batch is full. Producer has to call Flush() to
		return false
	}

	if bc.numMessages == 0 {
		var sequenceID uint64
		if metadata.SequenceId != nil {
			sequenceID = *metadata.SequenceId
		} else {
			sequenceID = GetAndAdd(sequenceIDGenerator, 1)
		}
		bc.msgMetadata.SequenceId = proto.Uint64(sequenceID)
		bc.msgMetadata.PublishTime = proto.Uint64(TimestampMillis(time.Now()))
		bc.msgMetadata.ProducerName = &bc.producerName
		bc.msgMetadata.ReplicateTo = replicateTo
		bc.msgMetadata.PartitionKey = metadata.PartitionKey

		if deliverAt.UnixNano() > 0 {
			bc.msgMetadata.DeliverAtTime = proto.Int64(int64(TimestampMillis(deliverAt)))
		}

		bc.cmdSend.Send.SequenceId = proto.Uint64(sequenceID)
	}
	addSingleMessageToBatch(bc.buffer, metadata, payload)

	bc.numMessages++
	bc.callbacks = append(bc.callbacks, callback)
	return true
}

func (bc *batchContainer) reset() {
	bc.numMessages = 0
	bc.buffer.Clear()
	bc.callbacks = []interface{}{}
	bc.msgMetadata.ReplicateTo = nil
	bc.msgMetadata.DeliverAtTime = nil
}

// Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
func (bc *batchContainer) Flush() (
	batchData Buffer, sequenceID uint64, callbacks []interface{},
) {
	if bc.numMessages == 0 {
		// No-Op for empty batch
		return nil, 0, nil
	}
	bc.log.Debug("BatchBuilder flush: messages: ", bc.numMessages)

	bc.msgMetadata.NumMessagesInBatch = proto.Int32(int32(bc.numMessages))
	bc.cmdSend.Send.NumMessages = proto.Int32(int32(bc.numMessages))

	uncompressedSize := bc.buffer.ReadableBytes()
	bc.msgMetadata.UncompressedSize = &uncompressedSize

	buffer := bc.buffersPool.GetBuffer()
	if buffer == nil {
		buffer = NewBuffer(int(uncompressedSize * 3 / 2))
	}
	serializeBatch(
		buffer, bc.cmdSend, bc.msgMetadata, bc.buffer, bc.compressionProvider,
	)

	callbacks = bc.callbacks
	sequenceID = bc.cmdSend.Send.GetSequenceId()
	bc.reset()
	return buffer, sequenceID, callbacks
}

// FlushBatches only for multiple batches container
func (bc *batchContainer) FlushBatches() (
	batchData []Buffer, sequenceID []uint64, callbacks [][]interface{},
) {
	panic("single batch container not support FlushBatches(), please use Flush() instead")
}

// batchContainer as a single batch container
func (bc *batchContainer) IsMultiBatches() bool {
	return false
}

func (bc *batchContainer) Close() error {
	return bc.compressionProvider.Close()
}

func getCompressionProvider(
	compressionType pb.CompressionType,
	level compression.Level,
) compression.Provider {
	switch compressionType {
	case pb.CompressionType_NONE:
		return compression.NewNoopProvider()
	case pb.CompressionType_LZ4:
		return compression.NewLz4Provider()
	case pb.CompressionType_ZLIB:
		return compression.NewZLibProvider()
	case pb.CompressionType_ZSTD:
		return compression.NewZStdProvider(level)
	default:
		panic("unsupported compression type")
	}
}

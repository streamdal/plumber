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
	"encoding/base64"
	"sort"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

/**
 * Key based batch message container
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into multiple batch messages:
 * [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
 */

// keyBasedBatches is a simple concurrent-safe map for the batchContainer type
type keyBasedBatches struct {
	containers map[string]*batchContainer
	l          *sync.RWMutex
}

// keyBasedBatchContainer wraps the objects needed to key based batch.
// keyBasedBatchContainer implement BatchBuilder as a multiple batches
// container.
type keyBasedBatchContainer struct {
	batches keyBasedBatches
	batchContainer
	compressionType pb.CompressionType
	level           compression.Level
}

// newKeyBasedBatches init a keyBasedBatches
func newKeyBasedBatches() keyBasedBatches {
	return keyBasedBatches{
		containers: map[string]*batchContainer{},
		l:          &sync.RWMutex{},
	}
}

func (h *keyBasedBatches) Add(key string, val *batchContainer) {
	h.l.Lock()
	defer h.l.Unlock()
	h.containers[key] = val
}

func (h *keyBasedBatches) Del(key string) {
	h.l.Lock()
	defer h.l.Unlock()
	delete(h.containers, key)
}

func (h *keyBasedBatches) Val(key string) *batchContainer {
	h.l.RLock()
	defer h.l.RUnlock()
	return h.containers[key]
}

// NewKeyBasedBatchBuilder init batch builder and return BatchBuilder
// pointer. Build a new key based batch message container.
func NewKeyBasedBatchBuilder(
	maxMessages uint, maxBatchSize uint, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, logger log.Logger,
) (BatchBuilder, error) {

	bb := &keyBasedBatchContainer{
		batches: newKeyBasedBatches(),
		batchContainer: newBatchContainer(
			maxMessages, maxBatchSize, producerName, producerID,
			compressionType, level, bufferPool, logger,
		),
		compressionType: compressionType,
		level:           level,
	}

	if compressionType != pb.CompressionType_NONE {
		bb.msgMetadata.Compression = &compressionType
	}

	return bb, nil
}

// IsFull check if the size in the current batch exceeds the maximum size allowed by the batch
func (bc *keyBasedBatchContainer) IsFull() bool {
	return bc.numMessages >= bc.maxMessages || bc.buffer.ReadableBytes() > uint32(bc.maxBatchSize)
}

func (bc *keyBasedBatchContainer) IsMultiBatches() bool {
	return true
}

func (bc *keyBasedBatchContainer) hasSpace(payload []byte) bool {
	msgSize := uint32(len(payload))
	return bc.numMessages > 0 && (bc.buffer.ReadableBytes()+msgSize) > uint32(bc.maxBatchSize)
}

// Add will add single message to key-based batch with message key.
func (bc *keyBasedBatchContainer) Add(
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

	var msgKey = getMessageKey(metadata)
	batchPart := bc.batches.Val(msgKey)
	if batchPart == nil {
		// create batchContainer for new key
		t := newBatchContainer(
			bc.maxMessages, bc.maxBatchSize, bc.producerName, bc.producerID,
			bc.compressionType, bc.level, bc.buffersPool, bc.log,
		)
		batchPart = &t
		bc.batches.Add(msgKey, &t)
	}

	// add message to batch container
	batchPart.Add(
		metadata, sequenceIDGenerator, payload, callback, replicateTo,
		deliverAt,
	)
	addSingleMessageToBatch(bc.buffer, metadata, payload)

	bc.numMessages++
	bc.callbacks = append(bc.callbacks, callback)
	return true
}

func (bc *keyBasedBatchContainer) reset() {
	bc.batches.l.RLock()
	defer bc.batches.l.RUnlock()
	for _, container := range bc.batches.containers {
		container.reset()
	}
	bc.numMessages = 0
	bc.buffer.Clear()
	bc.callbacks = []interface{}{}
	bc.msgMetadata.ReplicateTo = nil
	bc.msgMetadata.DeliverAtTime = nil
	bc.batches.containers = map[string]*batchContainer{}
}

// Flush all the messages buffered in multiple batches and wait until all
// messages have been successfully persisted.
func (bc *keyBasedBatchContainer) FlushBatches() (
	batchesData []Buffer, sequenceIDs []uint64, callbacks [][]interface{},
) {
	if bc.numMessages == 0 {
		// No-Op for empty batch
		return nil, nil, nil
	}

	bc.log.Debug("keyBasedBatchContainer flush: messages: ", bc.numMessages)
	var batchesLen = len(bc.batches.containers)
	var idx = 0
	sortedKeys := make([]string, 0, batchesLen)

	batchesData = make([]Buffer, batchesLen)
	sequenceIDs = make([]uint64, batchesLen)
	callbacks = make([][]interface{}, batchesLen)

	bc.batches.l.RLock()
	defer bc.batches.l.RUnlock()
	for k := range bc.batches.containers {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		container := bc.batches.containers[k]
		b, s, c := container.Flush()
		if b != nil {
			batchesData[idx] = b
			sequenceIDs[idx] = s
			callbacks[idx] = c
		}
		idx++
	}

	bc.reset()
	return batchesData, sequenceIDs, callbacks
}

func (bc *keyBasedBatchContainer) Flush() (
	batchData Buffer, sequenceID uint64, callbacks []interface{},
) {
	panic("multi batches container not support Flush(), please use FlushBatches() instead")
}

func (bc *keyBasedBatchContainer) Close() error {
	return bc.compressionProvider.Close()
}

// getMessageKey extracts message key from message metadata.
// If the OrderingKey exists, the base64-encoded string is returned,
// otherwise the PartitionKey is returned.
func getMessageKey(metadata *pb.SingleMessageMetadata) string {
	if k := metadata.GetOrderingKey(); k != nil {
		return base64.StdEncoding.EncodeToString(k)
	}
	return metadata.GetPartitionKey()
}

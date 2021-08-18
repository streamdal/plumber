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
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

type messageID struct {
	ledgerID     int64
	entryID      int64
	batchIdx     int32
	partitionIdx int32
}

var latestMessageID = messageID{
	ledgerID:     math.MaxInt64,
	entryID:      math.MaxInt64,
	batchIdx:     -1,
	partitionIdx: -1,
}

var earliestMessageID = messageID{
	ledgerID:     -1,
	entryID:      -1,
	batchIdx:     -1,
	partitionIdx: -1,
}

type trackingMessageID struct {
	messageID

	tracker      *ackTracker
	consumer     acker
	receivedTime time.Time
}

func (id trackingMessageID) Undefined() bool {
	return id == trackingMessageID{}
}

func (id trackingMessageID) Ack() {
	if id.consumer == nil {
		return
	}
	if id.ack() {
		id.consumer.AckID(id)
	}
}

func (id trackingMessageID) Nack() {
	if id.consumer == nil {
		return
	}
	id.consumer.NackID(id)
}

func (id trackingMessageID) ack() bool {
	if id.tracker != nil && id.batchIdx > -1 {
		return id.tracker.ack(int(id.batchIdx))
	}
	return true
}

func (id messageID) isEntryIDValid() bool {
	return id.entryID >= 0
}

func (id messageID) greater(other messageID) bool {
	if id.ledgerID != other.ledgerID {
		return id.ledgerID > other.ledgerID
	}

	if id.entryID != other.entryID {
		return id.entryID > other.entryID
	}

	return id.batchIdx > other.batchIdx
}

func (id messageID) equal(other messageID) bool {
	return id.ledgerID == other.ledgerID &&
		id.entryID == other.entryID &&
		id.batchIdx == other.batchIdx
}

func (id messageID) greaterEqual(other messageID) bool {
	return id.equal(other) || id.greater(other)
}

func (id messageID) Serialize() []byte {
	msgID := &pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(id.ledgerID)),
		EntryId:    proto.Uint64(uint64(id.entryID)),
		BatchIndex: proto.Int32(id.batchIdx),
		Partition:  proto.Int32(id.partitionIdx),
	}
	data, _ := proto.Marshal(msgID)
	return data
}

func (id messageID) LedgerID() int64 {
	return id.ledgerID
}

func (id messageID) EntryID() int64 {
	return id.entryID
}

func (id messageID) BatchIdx() int32 {
	return id.batchIdx
}

func (id messageID) PartitionIdx() int32 {
	return id.partitionIdx
}

func (id messageID) String() string {
	return fmt.Sprintf("%d:%d:%d", id.ledgerID, id.entryID, id.partitionIdx)
}

func deserializeMessageID(data []byte) (MessageID, error) {
	msgID := &pb.MessageIdData{}
	err := proto.Unmarshal(data, msgID)
	if err != nil {
		return nil, err
	}
	id := newMessageID(
		int64(msgID.GetLedgerId()),
		int64(msgID.GetEntryId()),
		msgID.GetBatchIndex(),
		msgID.GetPartition(),
	)
	return id, nil
}

func newMessageID(ledgerID int64, entryID int64, batchIdx int32, partitionIdx int32) MessageID {
	return messageID{
		ledgerID:     ledgerID,
		entryID:      entryID,
		batchIdx:     batchIdx,
		partitionIdx: partitionIdx,
	}
}

func newTrackingMessageID(ledgerID int64, entryID int64, batchIdx int32, partitionIdx int32,
	tracker *ackTracker) trackingMessageID {
	return trackingMessageID{
		messageID: messageID{
			ledgerID:     ledgerID,
			entryID:      entryID,
			batchIdx:     batchIdx,
			partitionIdx: partitionIdx,
		},
		tracker:      tracker,
		receivedTime: time.Now(),
	}
}

func toTrackingMessageID(msgID MessageID) (trackingMessageID, bool) {
	if mid, ok := msgID.(messageID); ok {
		return trackingMessageID{
			messageID:    mid,
			receivedTime: time.Now(),
		}, true
	} else if mid, ok := msgID.(trackingMessageID); ok {
		return mid, true
	} else {
		return trackingMessageID{}, false
	}
}

func timeFromUnixTimestampMillis(timestamp uint64) time.Time {
	ts := int64(timestamp) * int64(time.Millisecond)
	seconds := ts / int64(time.Second)
	nanos := ts - (seconds * int64(time.Second))
	return time.Unix(seconds, nanos)
}

type message struct {
	publishTime         time.Time
	eventTime           time.Time
	key                 string
	orderingKey         string
	producerName        string
	payLoad             []byte
	msgID               MessageID
	properties          map[string]string
	topic               string
	replicationClusters []string
	replicatedFrom      string
	redeliveryCount     uint32
	schema              Schema
}

func (msg *message) Topic() string {
	return msg.topic
}

func (msg *message) Properties() map[string]string {
	return msg.properties
}

func (msg *message) Payload() []byte {
	return msg.payLoad
}

func (msg *message) ID() MessageID {
	return msg.msgID
}

func (msg *message) PublishTime() time.Time {
	return msg.publishTime
}

func (msg *message) EventTime() time.Time {
	return msg.eventTime
}

func (msg *message) Key() string {
	return msg.key
}

func (msg *message) OrderingKey() string {
	return msg.orderingKey
}

func (msg *message) RedeliveryCount() uint32 {
	return msg.redeliveryCount
}

func (msg *message) IsReplicated() bool {
	return msg.replicatedFrom != ""
}

func (msg *message) GetReplicatedFrom() string {
	return msg.replicatedFrom
}

func (msg *message) GetSchemaValue(v interface{}) error {
	return msg.schema.Decode(msg.payLoad, v)
}

func (msg *message) ProducerName() string {
	return msg.producerName
}

func newAckTracker(size int) *ackTracker {
	var batchIDs *big.Int
	if size <= 64 {
		shift := uint32(64 - size)
		setBits := ^uint64(0) >> shift
		batchIDs = new(big.Int).SetUint64(setBits)
	} else {
		batchIDs, _ = new(big.Int).SetString(strings.Repeat("1", size), 2)
	}
	return &ackTracker{
		size:     size,
		batchIDs: batchIDs,
	}
}

type ackTracker struct {
	sync.Mutex
	size     int
	batchIDs *big.Int
}

func (t *ackTracker) ack(batchID int) bool {
	if batchID < 0 {
		return true
	}
	t.Lock()
	defer t.Unlock()
	t.batchIDs = t.batchIDs.SetBit(t.batchIDs, batchID, 0)
	return len(t.batchIDs.Bits()) == 0
}

func (t *ackTracker) completed() bool {
	t.Lock()
	defer t.Unlock()
	return len(t.batchIDs.Bits()) == 0
}

package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

type ConfirmationStatus struct {
	message      message.StreamMessage
	producerID   uint8
	publishingId int64
	confirmed    bool
	err          error
	errorCode    uint16
	linkedTo     []*ConfirmationStatus
}

func (cs *ConfirmationStatus) IsConfirmed() bool {
	return cs.confirmed
}

func (cs *ConfirmationStatus) GetProducerID() uint8 {
	return cs.producerID
}

func (cs *ConfirmationStatus) GetPublishingId() int64 {
	return cs.publishingId
}

func (cs *ConfirmationStatus) GetError() error {
	return cs.err
}

func (cs *ConfirmationStatus) LinkedMessages() []*ConfirmationStatus {
	return cs.linkedTo
}

func (cs *ConfirmationStatus) GetMessage() message.StreamMessage {
	return cs.message
}

func (cs *ConfirmationStatus) GetErrorCode() uint16 {
	return cs.errorCode
}

type pendingMessagesSequence struct {
	messages []messageSequence
	size     int
}

type messageSequence struct {
	messageBytes     []byte
	unCompressedSize int
	publishingId     int64
}

type Producer struct {
	id                  uint8
	options             *ProducerOptions
	onClose             onInternalClose
	unConfirmedMessages map[int64]*ConfirmationStatus
	sequence            int64
	mutex               *sync.Mutex
	mutexPending        *sync.Mutex
	publishConfirm      chan []*ConfirmationStatus
	closeHandler        chan Event
	status              int

	/// needed for the async publish
	messageSequenceCh chan messageSequence
	pendingMessages   pendingMessagesSequence
}

type ProducerOptions struct {
	client               *Client
	streamName           string
	Name                 string      // Producer name, it is useful to handle deduplication messages
	QueueSize            int         // Internal queue to handle back-pressure, low value reduces the back-pressure on the server
	BatchSize            int         // It is the batch-unCompressedSize aggregation, low value reduce the latency, high value increase the throughput
	BatchPublishingDelay int         // Period to send a batch of messages.
	SubEntrySize         int         // Size of sub Entry, to aggregate more subEntry using one publishing id
	Compression          Compression // Compression type, it is valid only if SubEntrySize > 1
}

func (po *ProducerOptions) SetProducerName(name string) *ProducerOptions {
	po.Name = name
	return po
}

func (po *ProducerOptions) SetQueueSize(size int) *ProducerOptions {
	po.QueueSize = size
	return po
}

func (po *ProducerOptions) SetBatchSize(size int) *ProducerOptions {
	po.BatchSize = size
	return po
}

func (po *ProducerOptions) SetBatchPublishingDelay(size int) *ProducerOptions {
	po.BatchPublishingDelay = size
	return po
}

func (po *ProducerOptions) SetSubEntrySize(size int) *ProducerOptions {
	po.SubEntrySize = size
	return po
}

func (po *ProducerOptions) SetCompression(compression Compression) *ProducerOptions {
	po.Compression = compression
	return po
}

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		QueueSize:            defaultQueuePublisherSize,
		BatchSize:            defaultBatchSize,
		BatchPublishingDelay: defaultBatchPublishingDelay,
		SubEntrySize:         1,
		Compression:          Compression{},
	}
}

func (producer *Producer) GetUnConfirmed() map[int64]*ConfirmationStatus {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages
}

func (producer *Producer) addUnConfirmed(sequence int64, message message.StreamMessage, producerID uint8) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.unConfirmedMessages[sequence] = &ConfirmationStatus{
		message:      message,
		producerID:   producerID,
		publishingId: sequence,
		confirmed:    false,
	}
}

func (po *ProducerOptions) isSubEntriesBatching() bool {
	return po.SubEntrySize > 1
}

func (producer *Producer) removeUnConfirmed(sequence int64) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	delete(producer.unConfirmedMessages, sequence)
}

func (producer *Producer) lenUnConfirmed() int {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return len(producer.unConfirmedMessages)
}

func (producer *Producer) lenPendingMessages() int {
	producer.mutexPending.Lock()
	defer producer.mutexPending.Unlock()
	return len(producer.pendingMessages.messages)
}

func (producer *Producer) getUnConfirmed(sequence int64) *ConfirmationStatus {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages[sequence]
}

func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*ConfirmationStatus)
	producer.publishConfirm = ch
	return ch
}

func (producer *Producer) NotifyClose() ChannelClose {
	ch := make(chan Event, 1)
	producer.closeHandler = ch
	return ch
}

func (producer *Producer) GetOptions() *ProducerOptions {
	return producer.options
}

func (producer *Producer) GetBroker() *Broker {
	return producer.options.client.broker
}
func (producer *Producer) setStatus(status int) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.status = status
}

func (producer *Producer) getStatus() int {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.status
}

func (producer *Producer) sendBufferedMessages() {

	if len(producer.pendingMessages.messages) > 0 {
		err := producer.internalBatchSend(producer.pendingMessages.messages)
		if err != nil {
			return
		}
		producer.pendingMessages.messages = producer.pendingMessages.messages[:0]
		producer.pendingMessages.size = initBufferPublishSize
	}
}
func (producer *Producer) startPublishTask() {
	go func(ch chan messageSequence) {
		var ticker = time.NewTicker(time.Duration(producer.options.BatchPublishingDelay) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {

			case msg, running := <-ch:
				{
					if !running {
						producer.FlushUnConfirmedMessages()
						if producer.publishConfirm != nil {
							close(producer.publishConfirm)
							producer.publishConfirm = nil
						}
						if producer.closeHandler != nil {
							close(producer.closeHandler)
							producer.closeHandler = nil
						}
						return
					}
					producer.mutexPending.Lock()
					if producer.pendingMessages.size+msg.unCompressedSize >= producer.options.client.getTuneState().
						requestedMaxFrameSize {
						producer.sendBufferedMessages()
					}

					producer.pendingMessages.size += msg.unCompressedSize
					producer.pendingMessages.messages = append(producer.pendingMessages.messages, msg)
					if len(producer.pendingMessages.messages) >= (producer.options.BatchSize) {
						producer.sendBufferedMessages()
					}
					producer.mutexPending.Unlock()
				}

			case <-ticker.C:
				producer.mutexPending.Lock()
				producer.sendBufferedMessages()
				producer.mutexPending.Unlock()
			}

		}
	}(producer.messageSequenceCh)

}

func (producer *Producer) Send(streamMessage message.StreamMessage) error {
	msgBytes, err := streamMessage.MarshalBinary()
	if err != nil {
		return err
	}

	if len(msgBytes)+initBufferPublishSize > producer.options.client.getTuneState().requestedMaxFrameSize {
		return FrameTooLarge
	}

	sequence := producer.assignPublishingID(streamMessage)
	producer.addUnConfirmed(sequence, streamMessage, producer.id)

	if producer.getStatus() == open {
		producer.messageSequenceCh <- messageSequence{
			messageBytes:     msgBytes,
			unCompressedSize: len(msgBytes),
			publishingId:     sequence,
		}
	} else {
		return fmt.Errorf("producer id: %d  closed", producer.id)
	}

	return nil
}

func (producer *Producer) assignPublishingID(message message.StreamMessage) int64 {
	sequence := message.GetPublishingId()
	// in case of sub entry the deduplication is disabled
	if !message.HasPublishingId() || producer.options.isSubEntriesBatching() {
		sequence = atomic.AddInt64(&producer.sequence, 1)
	}
	return sequence
}

func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	var messagesSequence = make([]messageSequence, len(batchMessages))
	totalBufferToSend := 0
	for i, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return err
		}
		sequence := producer.assignPublishingID(batchMessage)
		totalBufferToSend += len(messageBytes)
		messagesSequence[i] = messageSequence{
			messageBytes:     messageBytes,
			unCompressedSize: len(messageBytes),
			publishingId:     sequence,
		}

		producer.addUnConfirmed(sequence, batchMessage, producer.id)
	}

	if totalBufferToSend+initBufferPublishSize > producer.options.client.tuneState.requestedMaxFrameSize {
		for _, msg := range messagesSequence {

			unConfirmedMessage := producer.getUnConfirmed(msg.publishingId)

			//producer.mutex.Lock()
			if producer.publishConfirm != nil {
				unConfirmedMessage.err = FrameTooLarge
				unConfirmedMessage.errorCode = responseCodeFrameTooLarge
				producer.publishConfirm <- []*ConfirmationStatus{unConfirmedMessage}
			}
			//producer.mutex.Unlock()
			producer.removeUnConfirmed(msg.publishingId)
		}

		return FrameTooLarge
	}

	return producer.internalBatchSend(messagesSequence)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}
func (producer *Producer) internalBatchSend(messagesSequence []messageSequence) error {
	return producer.internalBatchSendProdId(messagesSequence, producer.GetID())
}

func (producer *Producer) simpleAggregation(messagesSequence []messageSequence, b *bufio.Writer) {
	for _, msg := range messagesSequence {
		r := msg.messageBytes
		writeBLong(b, msg.publishingId) // publishingId
		writeBInt(b, len(r))            // len
		b.Write(r)
	}
}

func (producer *Producer) subEntryAggregation(aggregation subEntries, b *bufio.Writer, compression Compression) {
	/// 51 messages
	// aggregation.items == (5 --> [10] messages) + (1 --> [1]message)
	for _, entry := range aggregation.items {
		writeBLong(b, entry.publishingId)
		writeBByte(b, 0x80|
			compression.value<<4) // 1=SubBatchEntryType:1,CompressionType:3,Reserved:4,
		writeBShort(b, int16(len(entry.messages)))
		writeBInt(b, entry.unCompressedSize)
		writeBInt(b, entry.sizeInBytes)
		b.Write(entry.dataInBytes)
	}
}

func (producer *Producer) aggregateEntities(msgs []messageSequence, size int, compression Compression) (subEntries, error) {
	subEntries := subEntries{}

	var entry *subEntry
	for _, msg := range msgs {
		if len(subEntries.items) == 0 || len(entry.messages) >= size {
			entry = &subEntry{}
			entry.publishingId = -1
			subEntries.items = append(subEntries.items, entry)
		}
		entry.messages = append(entry.messages, msg)
		binary := msg.messageBytes
		entry.unCompressedSize += len(binary) + 4

		// in case of subEntry we need to pick only one publishingId
		// we peek the first one of the entries
		// suppose you have 10 messages with publishingId [5..15]
		if entry.publishingId < 0 {
			entry.publishingId = msg.publishingId
		}

		/// since there is only one publishingId
		// the others publishingId(s) are linked
		// so the client confirms all the messages
		//when the client receives the confirmation form the server
		// see: server_frame:handleConfirm/2
		// suppose you have 10 messages with publishingId [5..15]
		// the message 5 is linked to 6,7,8,9..15

		if entry.publishingId != msg.publishingId {
			unConfirmed := producer.getUnConfirmed(entry.publishingId)
			if unConfirmed != nil {
				unConfirmed.linkedTo =
					append(unConfirmed.linkedTo,
						producer.getUnConfirmed(msg.publishingId))
			}
		}
	}

	compressByValue(compression.value).Compress(&subEntries)

	return subEntries, nil
}

/// the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error, see

func (producer *Producer) internalBatchSendProdId(messagesSequence []messageSequence, producerID uint8) error {
	producer.options.client.socket.mutex.Lock()
	defer producer.options.client.socket.mutex.Unlock()
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	var msgLen int
	var aggregation subEntries

	if producer.options.isSubEntriesBatching() {
		var err error
		aggregation, err = producer.aggregateEntities(messagesSequence, producer.options.SubEntrySize,
			producer.options.Compression)
		if err != nil {
			return err
		}
		msgLen += ((8 + 1 + 2 + 4 + 4) * len(aggregation.items)) + aggregation.totalSizeInBytes
	}

	if !producer.options.isSubEntriesBatching() {
		for _, msg := range messagesSequence {
			msgLen += msg.unCompressedSize + 8 + 4
		}
	}

	frameHeaderLength := initBufferPublishSize
	length := frameHeaderLength + msgLen
	//var b = bytes.NewBuffer(make([]byte, 0, length+4))

	writeBProtocolHeader(producer.options.client.socket.writer, length, commandPublish)
	writeBByte(producer.options.client.socket.writer, producerID)
	numberOfMessages := len(messagesSequence)
	numberOfMessages = numberOfMessages / producer.options.SubEntrySize
	if len(messagesSequence)%producer.options.SubEntrySize != 0 {
		numberOfMessages += 1
	}

	writeBInt(producer.options.client.socket.writer, numberOfMessages) //toExcluded - fromInclude

	if producer.options.isSubEntriesBatching() {
		producer.subEntryAggregation(aggregation, producer.options.client.socket.writer, producer.options.Compression)
	}

	if !producer.options.isSubEntriesBatching() {
		producer.simpleAggregation(messagesSequence, producer.options.client.socket.writer)
	}

	err := producer.options.client.socket.writer.Flush() //writeAndFlush(b.Bytes())
	if err != nil {
		// This sleep is need to wait the
		// 800 milliseconds to flush all the pending messages

		producer.setStatus(closed)
		producer.FlushUnConfirmedMessages()
		return err
	}
	return nil
}

func (producer *Producer) FlushUnConfirmedMessages() {
	producer.mutex.Lock()
	if producer.publishConfirm != nil {
		for _, msg := range producer.unConfirmedMessages {
			msg.confirmed = false
			msg.err = ConnectionClosed
			msg.errorCode = connectionCloseError
			producer.publishConfirm <- []*ConfirmationStatus{msg}
			delete(producer.unConfirmedMessages, msg.publishingId)
		}
	}
	producer.mutex.Unlock()
}

func (producer *Producer) GetLastPublishingId() (int64, error) {
	return producer.options.client.queryPublisherSequence(producer.GetName(), producer.GetStreamName())
}
func (producer *Producer) Close() error {
	if producer.getStatus() == closed {
		return AlreadyClosed
	}

	producer.waitForInflightMessages()
	producer.setStatus(closed)

	if !producer.options.client.socket.isOpen() {
		return fmt.Errorf("tcp connection is closed")
	}

	err := producer.options.client.deletePublisher(producer.id)
	if err != nil {
		logs.LogError("error delete Publisher on closing: %s", err)
	}
	if producer.options.client.coordinator.ProducersCount() == 0 {
		err := producer.options.client.Close()
		if err != nil {
			logs.LogError("error during closing client: %s", err)
		}
	}

	if producer.onClose != nil {
		ch := make(chan uint8, 1)
		ch <- producer.id
		producer.onClose(ch)
		close(ch)
	}

	close(producer.messageSequenceCh)

	return nil
}

func (producer *Producer) waitForInflightMessages() {
	// during the close there cloud be pending messages
	// it waits for producer.options.BatchPublishingDelay
	// to flush the last messages
	// see issues/103

	channelLength := len(producer.messageSequenceCh)
	pendingMessagesLen := producer.lenPendingMessages()
	tentatives := 0

	for (channelLength > 0 || pendingMessagesLen > 0 || producer.lenUnConfirmed() > 0) && tentatives < 3 {
		logs.LogDebug("waitForInflightMessages, channel: %d - pending messages len: %d - unconfirmed len: %d - retry: %d",
			channelLength, pendingMessagesLen,
			producer.lenUnConfirmed(), tentatives)
		time.Sleep(time.Duration(2*producer.options.BatchPublishingDelay) * time.Millisecond)
		channelLength = len(producer.messageSequenceCh)
		pendingMessagesLen = producer.lenPendingMessages()
		tentatives++
	}
}

func (producer *Producer) GetStreamName() string {
	if producer.options == nil {
		return ""
	}
	return producer.options.streamName
}

func (producer *Producer) GetName() string {
	if producer.options == nil {
		return ""
	}
	return producer.options.Name
}

func (c *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := c.coordinator.NewResponse(CommandDeletePublisher)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, CommandDeletePublisher,
		correlationId)

	writeByte(b, publisherId)
	errWrite := c.handleWrite(b.Bytes(), resp)

	err := c.coordinator.RemoveProducerById(publisherId, Event{
		Command: CommandDeletePublisher,
		Reason:  "deletePublisher",
		Err:     nil,
	})
	if err != nil {
		logs.LogWarn("producer id: %d already removed", publisherId)
	}

	return errWrite.Err
}

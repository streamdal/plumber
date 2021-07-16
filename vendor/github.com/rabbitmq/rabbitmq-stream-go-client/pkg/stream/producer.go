package stream

import (
	"bytes"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

type UnConfirmedMessage struct {
	Message    message.StreamMessage
	ProducerID uint8
	SequenceID int64
	Confirmed  bool
	Err        error
}

type pendingMessagesSequence struct {
	messages []messageSequence
	size     int
}

type messageSequence struct {
	message      message.StreamMessage
	size         int
	publishingId int64
}

type Producer struct {
	ID                  uint8
	options             *ProducerOptions
	onClose             onInternalClose
	unConfirmedMessages map[int64]*UnConfirmedMessage
	sequence            int64
	mutex               *sync.Mutex
	publishConfirm      chan []*UnConfirmedMessage
	publishError        chan PublishError
	closeHandler        chan Event
	status              int

	/// needed for the async publish
	messageSequenceCh chan messageSequence
	pendingMessages   pendingMessagesSequence
}

type ProducerOptions struct {
	client     *Client
	streamName string
	Name       string
	QueueSize  int
	BatchSize  int
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

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		QueueSize: 10000,
		BatchSize: 100,
	}
}

func (producer *Producer) GetUnConfirmed() map[int64]*UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages
}

func (producer *Producer) addUnConfirmed(sequence int64, message message.StreamMessage, producerID uint8) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.unConfirmedMessages[sequence] = &UnConfirmedMessage{
		Message:    message,
		ProducerID: producerID,
		SequenceID: sequence,
		Confirmed:  false,
	}
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

func (producer *Producer) getUnConfirmed(sequence int64) *UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages[sequence]
}

func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*UnConfirmedMessage)
	producer.publishConfirm = ch
	return ch
}

func (producer *Producer) NotifyPublishError() ChannelPublishError {
	ch := make(chan PublishError)
	producer.publishError = ch
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
		//logs.LogInfo("len %d",  len(producer.pendingMessages.messages))
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
		var ticker = time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {

			select {

			case msg, running := <-ch:
				{
					if !running {
						return
					}
					if producer.pendingMessages.size+msg.size >= producer.options.client.getTuneState().requestedMaxFrameSize {
						producer.sendBufferedMessages()
					}

					producer.pendingMessages.size += producer.pendingMessages.size + msg.size
					producer.pendingMessages.messages = append(producer.pendingMessages.messages, msg)
					if len(producer.pendingMessages.messages) >= producer.options.BatchSize {
						producer.sendBufferedMessages()
					}
				}

			case <-ticker.C:
				producer.sendBufferedMessages()
			}

		}
	}(producer.messageSequenceCh)

}

func (producer *Producer) Send(message message.StreamMessage) error {

	msgBytes, err := message.MarshalBinary()
	if err != nil {
		return err
	}

	if len(msgBytes)+initBufferPublishSize > producer.options.client.getTuneState().requestedMaxFrameSize {
		return FrameTooLarge
	}
	sequence := producer.getPublishingID(message)
	producer.addUnConfirmed(sequence, message, producer.ID)

	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d  closed", producer.ID)
	}

	producer.messageSequenceCh <- messageSequence{
		message:      message,
		size:         len(msgBytes),
		publishingId: sequence,
	}

	return nil
}

func (producer *Producer) getPublishingID(message message.StreamMessage) int64 {
	sequence := message.GetPublishingId()
	if message.GetPublishingId() < 0 {
		sequence = atomic.AddInt64(&producer.sequence, 1)
	}
	return sequence
}

func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	var messagesSequence = make([]messageSequence, len(batchMessages))
	for i, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return nil
		}
		sequence := producer.getPublishingID(batchMessage)
		messagesSequence[i] = messageSequence{
			message:      batchMessage,
			size:         len(messageBytes),
			publishingId: sequence,
		}
		producer.addUnConfirmed(sequence, batchMessage, producer.ID)
	}

	return producer.internalBatchSend(messagesSequence)
}

func (producer *Producer) internalBatchSend(messagesSequence []messageSequence) error {
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.ID)
	}

	var msgLen int
	for _, msg := range messagesSequence {
		msgLen += msg.size + 8 + 4
	}

	frameHeaderLength := initBufferPublishSize
	length := frameHeaderLength + msgLen
	publishId := producer.ID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, publishId)
	writeInt(b, len(messagesSequence)) //toExcluded - fromInclude

	for _, msg := range messagesSequence {
		r, _ := msg.message.MarshalBinary()
		writeLong(b, msg.publishingId) // publishingId
		writeInt(b, len(r))            // len
		b.Write(r)
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.getTuneState().requestedMaxFrameSize {
		return lookErrorCode(responseCodeFrameTooLarge)
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	if err != nil {
		// This sleep is need to wait the
		// 200 milliseconds to flush all the pending messages
		time.Sleep(800 * time.Millisecond)
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
			msg.Confirmed = false
			producer.publishConfirm <- []*UnConfirmedMessage{msg}
			delete(producer.unConfirmedMessages, msg.SequenceID)
		}
	}
	producer.mutex.Unlock()
}

func (producer *Producer) Close() error {
	producer.setStatus(closed)
	if !producer.options.client.socket.isOpen() {
		return fmt.Errorf("connection already closed")
	}

	err := producer.options.client.deletePublisher(producer.ID)
	if err != nil {
		return err
	}
	if producer.options.client.coordinator.ProducersCount() == 0 {
		err := producer.options.client.Close()
		if err != nil {
			return err
		}
	}

	if producer.onClose != nil {
		ch := make(chan uint8, 1)
		ch <- producer.ID
		producer.onClose(ch)
		close(ch)
	}

	producer.mutex.Lock()
	if producer.publishConfirm != nil {
		close(producer.publishConfirm)
		producer.publishConfirm = nil
	}
	if producer.closeHandler != nil {
		close(producer.closeHandler)
		producer.closeHandler = nil
	}
	close(producer.messageSequenceCh)

	producer.mutex.Unlock()

	return nil
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

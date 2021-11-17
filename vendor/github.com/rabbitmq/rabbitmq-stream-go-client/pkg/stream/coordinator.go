package stream

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	counter          int
	producers        map[interface{}]interface{}
	consumers        map[interface{}]interface{}
	responses        map[interface{}]interface{}
	nextItemProducer uint8
	nextItemConsumer uint8
	mutex            *sync.Mutex
}

type Code struct {
	id uint16
}

type Response struct {
	code               chan Code
	data               chan interface{}
	messages           chan []*amqp.Message
	commandDescription string
	correlationid      int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		producers: make(map[interface{}]interface{}),
		consumers: make(map[interface{}]interface{}),
		responses: make(map[interface{}]interface{})}
}

// producersEnvironment
func (coordinator *Coordinator) NewProducer(
	parameters *ProducerOptions) (*Producer, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	size := 10000
	if parameters != nil {
		size = parameters.QueueSize
	}

	var lastId, err = coordinator.getNextProducerItem()
	if err != nil {
		return nil, err
	}
	var producer = &Producer{id: lastId,
		options:             parameters,
		mutex:               &sync.Mutex{},
		unConfirmedMessages: map[int64]*UnConfirmedMessage{},
		status:              open,
		messageSequenceCh:   make(chan messageSequence, size),
		pendingMessages: pendingMessagesSequence{
			messages: make([]messageSequence, 0),
			size:     initBufferPublishSize,
		}}
	coordinator.producers[lastId] = producer
	return producer, err
}

func (coordinator *Coordinator) RemoveConsumerById(id interface{}, reason Event) error {
	consumer, err := coordinator.GetConsumerById(id)
	if err != nil {
		return err
	}
	reason.StreamName = consumer.GetStreamName()
	reason.Name = consumer.GetName()

	if consumer.closeHandler != nil {
		consumer.closeHandler <- reason
		close(consumer.closeHandler)
	}

	return coordinator.removeById(id, coordinator.consumers)
}
func (coordinator *Coordinator) RemoveProducerById(id uint8, reason Event) error {

	producer, err := coordinator.GetProducerById(id)
	if err != nil {
		return err
	}
	reason.StreamName = producer.GetStreamName()
	reason.Name = producer.GetName()
	tentatives := 0
	for producer.lenUnConfirmed() > 0 && tentatives < 3 {
		time.Sleep(500 * time.Millisecond)
		tentatives++
	}
	producer.FlushUnConfirmedMessages()

	if producer.closeHandler != nil {
		producer.closeHandler <- reason
	}
	return coordinator.removeById(id, coordinator.producers)
}

func (coordinator *Coordinator) RemoveResponseById(id interface{}) error {
	resp, err := coordinator.GetResponseByName(fmt.Sprintf("%d", id))
	if err != nil {
		return err
	}

	err = coordinator.removeById(fmt.Sprintf("%d", id), coordinator.responses)
	close(resp.code)
	close(resp.data)
	close(resp.messages)
	return err
}

func (coordinator *Coordinator) ProducersCount() int {
	return coordinator.count(coordinator.producers)
}

/// response
func newResponse(commandDescription string) *Response {
	res := &Response{}
	res.commandDescription = commandDescription
	res.code = make(chan Code, 1)
	res.data = make(chan interface{})
	res.messages = make(chan []*amqp.Message, 100)
	return res
}

func (coordinator *Coordinator) NewResponseWitName(id string) *Response {
	coordinator.mutex.Lock()
	coordinator.counter++
	res := newResponse(id)
	res.correlationid = coordinator.counter
	coordinator.responses[id] = res
	coordinator.mutex.Unlock()
	return res
}

func (coordinator *Coordinator) NewResponse(commandId uint16, info ...string) *Response {
	description := lookUpCommand(commandId)
	if len(info) > 0 {
		description = fmt.Sprintf("%s, - %s", description, info[0])
	}

	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.counter++
	res := newResponse(description)
	res.correlationid = coordinator.counter
	coordinator.responses[strconv.Itoa(coordinator.counter)] = res
	return res
}

func (coordinator *Coordinator) GetResponseByName(id string) (*Response, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if coordinator.responses[id] == nil {
		return nil, errors.New("Response #{id} not found ")
	}
	switch coordinator.responses[id].(type) {
	case *Response:
		return coordinator.responses[id].(*Response), nil
	}

	return nil, nil
}

func (coordinator *Coordinator) RemoveResponseByName(id string) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if coordinator.responses[id] == nil {
		return errors.New("Response #{id} not found ")
	}
	coordinator.responses[id] = nil
	delete(coordinator.responses, id)
	return nil
}

/// Consumer functions
func (coordinator *Coordinator) NewConsumer(messagesHandler MessagesHandler,
	parameters *ConsumerOptions) *Consumer {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	var lastId, _ = coordinator.getNextConsumerItem()
	var item = &Consumer{ID: lastId, options: parameters,
		response:        newResponse(lookUpCommand(commandSubscribe)),
		status:          open,
		mutex:           &sync.Mutex{},
		MessagesHandler: messagesHandler,
	}

	coordinator.consumers[lastId] = item
	return item
}

func (coordinator *Coordinator) GetConsumerById(id interface{}) (*Consumer, error) {
	v, err := coordinator.getById(id, coordinator.consumers)
	if err != nil {
		return nil, err
	}
	return v.(*Consumer), err
}

func (coordinator *Coordinator) GetResponseById(id uint32) (*Response, error) {
	v, err := coordinator.getById(fmt.Sprintf("%d", id), coordinator.responses)
	if err != nil {
		return nil, err
	}
	return v.(*Response), err
}

func (coordinator *Coordinator) ConsumersCount() int {
	return coordinator.count(coordinator.consumers)
}

func (coordinator *Coordinator) GetProducerById(id interface{}) (*Producer, error) {
	v, err := coordinator.getById(id, coordinator.producers)
	if err != nil {
		return nil, err
	}
	return v.(*Producer), err
}

// general functions

func (coordinator *Coordinator) getById(id interface{}, refmap map[interface{}]interface{}) (interface{}, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if refmap[id] == nil {
		return nil, errors.New("item #{id} not found ")
	}
	return refmap[id], nil
}

func (coordinator *Coordinator) removeById(id interface{}, refmap map[interface{}]interface{}) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if refmap[id] == nil {
		return fmt.Errorf("remove failed, id %d not found", id)
	}
	refmap[id] = nil
	delete(refmap, id)
	return nil
}

func (coordinator *Coordinator) count(refmap map[interface{}]interface{}) int {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	return len(refmap)
}
func (coordinator *Coordinator) getNextProducerItem() (uint8, error) {
	if coordinator.nextItemProducer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.producers)
	}
	res := coordinator.nextItemProducer
	coordinator.nextItemProducer++
	return res, nil
}

func (coordinator *Coordinator) getNextConsumerItem() (uint8, error) {
	if coordinator.nextItemConsumer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.consumers)
	}
	res := coordinator.nextItemConsumer
	coordinator.nextItemConsumer++
	return res, nil
}

func (coordinator *Coordinator) reuseFreeId(refMap map[interface{}]interface{}) (byte, error) {
	maxValue := int(^uint8(0))
	var result byte
	for i := 0; i < maxValue; i++ {
		if refMap[byte(i)] == nil {
			return byte(i), nil
		}
		result++
	}
	if result >= ^uint8(0) {
		return 0, errors.New("No more items available")
	}
	return result, nil
}

func (coordinator *Coordinator) Producers() map[interface{}]interface{} {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	return coordinator.producers
}

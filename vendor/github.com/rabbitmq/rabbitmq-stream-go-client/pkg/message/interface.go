package message

import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

type StreamMessage interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
	SetPublishingId(id int64)
	GetPublishingId() int64
	HasPublishingId() bool
	GetData() [][]byte
	GetMessageProperties() *amqp.MessageProperties
	GetMessageAnnotations() amqp.Annotations
	GetApplicationProperties() map[string]interface{}

	// GetMessageHeader GetAMQPValue read only values see: rabbitmq-stream-go-client/issues/128
	GetMessageHeader() *amqp.MessageHeader
	GetAMQPValue() interface{}
}

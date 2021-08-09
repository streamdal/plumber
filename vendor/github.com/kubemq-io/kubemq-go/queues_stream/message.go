package queues_stream

import (
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueueMessage struct {
	*pb.QueueMessage
	*responseHandler
}

func (qm *QueueMessage) complete(clientId string) *QueueMessage {
	if qm.ClientID == "" {
		qm.ClientID = clientId
	}
	return qm
}

func (qm *QueueMessage) setResponseHandler(responseHandler *responseHandler) *QueueMessage {
	qm.responseHandler = responseHandler
	return qm
}

func NewQueueMessage() *QueueMessage {
	return &QueueMessage{
		QueueMessage: &pb.QueueMessage{
			MessageID:  "",
			ClientID:   "",
			Channel:    "",
			Metadata:   "",
			Body:       nil,
			Tags:       map[string]string{},
			Attributes: nil,
			Policy:     &pb.QueueMessagePolicy{},
		},
	}
}
func newQueueMessageFrom(msg *pb.QueueMessage) *QueueMessage {
	return &QueueMessage{
		QueueMessage: msg,
	}
}

// SetId - set queue message id, otherwise new random uuid will be set
func (qm *QueueMessage) SetId(id string) *QueueMessage {
	qm.MessageID = id
	return qm

}

// SetClientId - set queue message ClientId - mandatory if default grpcClient was not set
func (qm *QueueMessage) SetClientId(clientId string) *QueueMessage {
	qm.ClientID = clientId
	return qm
}

// SetChannel - set queue message Channel - mandatory if default Channel was not set
func (qm *QueueMessage) SetChannel(channel string) *QueueMessage {
	qm.Channel = channel
	return qm
}

// SetMetadata - set queue message metadata - mandatory if body field is empty
func (qm *QueueMessage) SetMetadata(metadata string) *QueueMessage {
	qm.Metadata = metadata
	return qm
}

// SetBody - set queue message body - mandatory if metadata field is empty
func (qm *QueueMessage) SetBody(body []byte) *QueueMessage {
	qm.Body = body
	return qm
}

// SetTags - set key value tags to queue message
func (qm *QueueMessage) SetTags(tags map[string]string) *QueueMessage {
	qm.Tags = map[string]string{}
	for key, value := range tags {
		qm.Tags[key] = value
	}
	return qm
}

// AddTag - add key value tags to query message
func (qm *QueueMessage) AddTag(key, value string) *QueueMessage {
	if qm.Tags == nil {
		qm.Tags = map[string]string{}
	}
	qm.Tags[key] = value
	return qm
}

// SetPolicyExpirationSeconds - set queue message expiration seconds, 0 never expires
func (qm *QueueMessage) SetPolicyExpirationSeconds(sec int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.ExpirationSeconds = int32(sec)
	return qm
}

// SetPolicyDelaySeconds - set queue message delivery delay in seconds, 0 , immediate delivery
func (qm *QueueMessage) SetPolicyDelaySeconds(sec int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.DelaySeconds = int32(sec)
	return qm
}

// SetPolicyMaxReceiveCount - set max delivery attempts before message will discard or re-route to a new queue
func (qm *QueueMessage) SetPolicyMaxReceiveCount(max int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.MaxReceiveCount = int32(max)
	return qm
}

// SetPolicyMaxReceiveQueue - set queue name to be routed once MaxReceiveCount is triggered, empty will discard the message
func (qm *QueueMessage) SetPolicyMaxReceiveQueue(channel string) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.MaxReceiveQueue = channel
	return qm
}

func (qm *QueueMessage) Ack() error {
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}
	return qm.responseHandler.AckOffsets(int64(qm.Attributes.Sequence))
}
func (qm *QueueMessage) NAck() error {
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}
	return qm.responseHandler.NAckOffsets(int64(qm.Attributes.Sequence))
}
func (qm *QueueMessage) ReQueue(channel string) error {
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}
	return qm.responseHandler.ReQueueOffsets(channel, int64(qm.Attributes.Sequence))
}

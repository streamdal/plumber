package queues_stream

import (
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type SubscribeRequest struct {
	Channels    []string
	MaxItems    int  `json:"max_items"`
	WaitTimeout int  `json:"wait_timeout"`
	AutoAck     bool `json:"auto_ack"`
}

func NewSubscribeRequest() *SubscribeRequest {
	return &SubscribeRequest{}
}

func (s *SubscribeRequest) SetChannels(channels ...string) *SubscribeRequest {
	s.Channels = channels
	return s
}

func (s *SubscribeRequest) SetMaxItems(maxItems int) *SubscribeRequest {
	s.MaxItems = maxItems
	return s
}

func (s *SubscribeRequest) SetWaitTimeout(waitTimeout int) *SubscribeRequest {
	s.WaitTimeout = waitTimeout
	return s
}

func (s *SubscribeRequest) SetAutoAck(autoAck bool) *SubscribeRequest {
	s.AutoAck = autoAck
	return s
}

func (s *SubscribeRequest) validateAndComplete(clientId string) ([]*pb.QueuesDownstreamRequest, error) {

	if len(s.Channels) == 0 {
		return nil, fmt.Errorf("request channels cannot be empty")
	}
	for i, channel := range s.Channels {
		if channel == "" {
			return nil, fmt.Errorf("request channel %d cannot be empty", i)
		}
	}
	if s.MaxItems < 0 {
		return nil, fmt.Errorf("request max items cannot be negative")
	}
	if s.WaitTimeout < 0 {
		return nil, fmt.Errorf("request wait timeout cannot be negative")
	}
	requestClientId := clientId
	if requestClientId == "" {
		requestClientId = uuid.New()
	}
	var requests []*pb.QueuesDownstreamRequest
	for _, channel := range s.Channels {
		requests = append(requests, &pb.QueuesDownstreamRequest{
			RequestID:       uuid.New(),
			ClientID:        requestClientId,
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Channel:         channel,
			MaxItems:        int32(s.MaxItems),
			WaitTimeout:     int32(s.WaitTimeout),
			AutoAck:         s.AutoAck,
		})
	}
	return requests, nil
}

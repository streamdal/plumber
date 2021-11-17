package queues_stream

import (
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

// PollRequest - Request parameters for Poll function
type PollRequest struct {
	Channel     string `json:"Channel"`
	MaxItems    int    `json:"max_items"`
	WaitTimeout int    `json:"wait_timeout"`
	AutoAck     bool   `json:"auto_ack"`
	OnErrorFunc func(err error)
	OnComplete  func()
}

func (p *PollRequest) SetOnErrorFunc(onErrorFunc func(err error)) *PollRequest {
	p.OnErrorFunc = onErrorFunc
	return p
}

func (p *PollRequest) SetOnComplete(onComplete func()) *PollRequest {
	p.OnComplete = onComplete
	return p
}

func NewPollRequest() *PollRequest {
	return &PollRequest{}
}
func (p *PollRequest) SetChannel(channel string) *PollRequest {
	p.Channel = channel
	return p
}

func (p *PollRequest) SetMaxItems(maxItems int) *PollRequest {
	p.MaxItems = maxItems
	return p
}

func (p *PollRequest) SetWaitTimeout(waitTimeout int) *PollRequest {
	p.WaitTimeout = waitTimeout
	return p
}

func (p *PollRequest) SetAutoAck(autoAck bool) *PollRequest {
	p.AutoAck = autoAck
	return p
}

func (p *PollRequest) validateAndComplete(clientId string) (*pb.QueuesDownstreamRequest, error) {
	if p.Channel == "" {
		return nil, fmt.Errorf("request channel cannot be empty")
	}
	if p.MaxItems < 0 {
		return nil, fmt.Errorf("request max items cannot be negative")
	}
	if p.WaitTimeout < 0 {
		return nil, fmt.Errorf("request wait timeout cannot be negative")
	}
	requestClientId := clientId
	if requestClientId == "" {
		requestClientId = uuid.New()
	}
	return &pb.QueuesDownstreamRequest{
		RequestID:       uuid.New(),
		ClientID:        requestClientId,
		RequestTypeData: pb.QueuesDownstreamRequestType_Get,
		Channel:         p.Channel,
		MaxItems:        int32(p.MaxItems),
		WaitTimeout:     int32(p.WaitTimeout),
		AutoAck:         p.AutoAck,
	}, nil
}

package queues_stream

import (
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

type AckAllRequest struct {
	requestID       string
	ClientID        string
	Channel         string
	WaitTimeSeconds int32
}

func NewAckAllRequest() *AckAllRequest {
	return &AckAllRequest{
		requestID:       uuid.New(),
		ClientID:        "",
		Channel:         "",
		WaitTimeSeconds: 0,
	}
}

func (req *AckAllRequest) SetClientId(clientId string) *AckAllRequest {
	req.ClientID = clientId
	return req
}

// SetChannel - set ack all queue message request channel - mandatory if default channel was not set
func (req *AckAllRequest) SetChannel(channel string) *AckAllRequest {
	req.Channel = channel
	return req
}

// SetWaitTimeSeconds - set ack all queue message request wait timout
func (req *AckAllRequest) SetWaitTimeSeconds(wait int) *AckAllRequest {
	req.WaitTimeSeconds = int32(wait)
	return req
}

func (req *AckAllRequest) validateAndComplete(clientId string) error {
	if req.ClientID == "" {
		req.ClientID = clientId
	}
	if req.Channel == "" {
		return fmt.Errorf("ack all must have a channel")
	}
	if req.ClientID == "" {
		return fmt.Errorf("ack all must have a clientId")
	}

	if req.WaitTimeSeconds <= 0 {
		return fmt.Errorf("queues subscription must have a wait time seconds >0")
	}

	return nil
}

type AckAllResponse struct {
	RequestID        string
	AffectedMessages uint64
	IsError          bool
	Error            string
}

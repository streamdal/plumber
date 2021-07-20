package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

type PlumberServer struct {
	AuthToken        string
	Connections      map[string]*protos.Connection
	ConnectionsMutex *sync.RWMutex
	Reads            map[string]*Read
	ReadsMutex       *sync.RWMutex
	Log              *logrus.Entry
}

type ErrorWrapper struct {
	Status *common.Status
}

func (e *ErrorWrapper) Error() string {
	return e.Status.Message
}

func CustomError(c common.Code, msg string) error {
	return &ErrorWrapper{
		Status: &common.Status{
			Code:      c,
			Message:   msg,
			RequestId: uuid.NewV4().String(),
		},
	}
}

func (p *PlumberServer) CreateRelay(ctx context.Context, req *protos.CreateRelayRequest) (*protos.CreateRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil
}

func (p *PlumberServer) UpdateRelay(ctx context.Context, req *protos.UpdateRelayRequest) (*protos.UpdateRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil
}

func (p *PlumberServer) StopRelay(ctx context.Context, req *protos.StopRelayRequest) (*protos.StopRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil
}

func (p *PlumberServer) DeleteRelay(ctx context.Context, req *protos.DeleteRelayRequest) (*protos.DeleteRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil
}

func (p *PlumberServer) validateRequest(auth *common.Auth) error {
	if auth == nil {
		return errors.New("auth cannot be nil")
	}

	if auth.Token != p.AuthToken {
		return errors.New("invalid token")
	}

	return nil
}

// getRead returns an in-progress read from the Read map
func (p *PlumberServer) getRead(readID string) *Read {
	p.ReadsMutex.RLock()
	defer p.ReadsMutex.RUnlock()

	r, _ := p.Reads[readID]

	return r
}

// setRead adds an in-progress read to the Read map
func (p *PlumberServer) setRead(readID string, read *Read) {
	p.ReadsMutex.Lock()
	defer p.ReadsMutex.Unlock()

	p.Reads[readID] = read
}

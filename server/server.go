package server

import (
	"errors"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/github"
	"github.com/batchcorp/plumber/server/types"
)

type PlumberServer struct {
	AuthToken        string
	PersistentConfig *config.Config
	ConnectionsMutex *sync.RWMutex
	ServicesMutex    *sync.RWMutex
	Reads            map[string]*Read
	ReadsMutex       *sync.RWMutex
	RelaysMutex      *sync.RWMutex
	GithubAuth       *github.UserCodeResponse
	GithubService    github.IGithub
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

// getRead returns an in-progress read from the Relay map
func (p *PlumberServer) getRelay(relayID string) *types.Relay {
	p.RelaysMutex.RLock()
	defer p.RelaysMutex.RUnlock()

	r, _ := p.PersistentConfig.Relays[relayID]

	return r
}

// setRelay adds an in-progress read to the Relay map
func (p *PlumberServer) setRelay(relayID string, relay *types.Relay) {
	p.RelaysMutex.Lock()
	defer p.RelaysMutex.Unlock()

	p.PersistentConfig.Relays[relayID] = relay
}

// getService returns an in-progress read from the Relay map
func (p *PlumberServer) getService(serviceID string) *types.Service {
	p.ServicesMutex.RLock()
	defer p.ServicesMutex.RUnlock()

	r, _ := p.PersistentConfig.Services[serviceID]

	return r
}

// setService adds an in-progress read to the Service map
func (p *PlumberServer) setService(serviceID string, svc *types.Service) {
	p.ServicesMutex.Lock()
	defer p.ServicesMutex.Unlock()

	p.PersistentConfig.Services[serviceID] = svc
}

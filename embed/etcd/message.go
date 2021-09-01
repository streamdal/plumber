package etcd

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	CreateConnection = "CreateConnection"
	UpdateConnection = "UpdateConnection"
	DeleteConnection = "DeleteConnection"

	CreateService = "CreateService"
	UpdateService = "UpdateService"
	DeleteService = "DeleteService"

	CreateSchema = "CreateSchema"
	UpdateSchema = "UpdateSchema"
	DeleteSchema = "DeleteSchema"

	CreateRelay = "CreateRelay"
	UpdateRelay = "UpdateRelay"
	DeleteRelay = "DeleteRelay"
)

var (
	ValidActions = []Action{CreateConnection, UpdateConnection, DeleteConnection}
)

type Action string

type Message struct {
	Action    Action
	Data      []byte // <- consumer decides what's in here based on action
	Metadata  map[string]string
	EmittedBy string
	EmittedAt time.Time // UTC
}

// MessageDeleteConnection is emitted and consumed during DeleteConnection actions
type MessageDeleteConnection struct {
	ConnectionID string `json:"connection_id"`
}

type MessageDeleteService struct {
	ServiceID string `json:"service_id"`
}

func (m *Message) Validate() error {
	if m == nil {
		return errors.New("message cannot be nil")
	}

	var found bool

	for _, v := range ValidActions {
		if m.Action == v {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("unrecognized action '%s'", m.Action)
	}

	if m.EmittedBy == "" {
		return errors.New("EmittedBy cannot be empty")
	}

	if m.EmittedAt.IsZero() {
		return errors.New("EmittedAt cannot be unset")
	}

	return nil
}

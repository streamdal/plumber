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

	UpdateConfig = "UpdateConfig"
)

var (
	ValidActions = []Action{
		CreateConnection, UpdateConnection, DeleteConnection,
		CreateService, UpdateService, DeleteService,
		CreateRelay, UpdateRelay, DeleteRelay,
		UpdateConfig,
	}
)

type Action string

type Message struct {
	Action    Action
	Data      []byte // <- consumer decides what's in here based on action
	Metadata  map[string]string
	EmittedBy string
	EmittedAt time.Time // UTC
}

// MessageUpdateConfig is emitted when a grpc.SetServerOptions() call is made
type MessageUpdateConfig struct {
	VCServiceToken string `json:"vsservice_token"`
}

// TODO: implement, this isn't being used anywhere at the moment
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

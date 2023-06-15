package bus

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	CreateConnection = "CreateConnection"
	UpdateConnection = "UpdateConnection"
	DeleteConnection = "DeleteConnection"

	CreateRelay = "CreateRelay"
	UpdateRelay = "UpdateRelay"
	DeleteRelay = "DeleteRelay"
	StopRelay   = "StopRelay"
	ResumeRelay = "ResumeRelay"

	CreateTunnel = "CreateTunnel"
	UpdateTunnel = "UpdateTunnel"
	DeleteTunnel = "DeleteTunnel"
	StopTunnel   = "StopTunnel"
	ResumeTunnel = "ResumeTunnel"

	UpdateConfig = "UpdateConfig"

	CreateRuleSet = "CreateRuleSet"
	UpdateRuleSet = "UpdateRuleSet"
	DeleteRuleSet = "DeleteRuleSet"

	Counter = "Counter"
)

var (
	ValidActions = []Action{
		CreateConnection, UpdateConnection, DeleteConnection,
		CreateRelay, UpdateRelay, DeleteRelay, StopRelay, ResumeRelay,
		CreateTunnel, UpdateTunnel, DeleteTunnel, StopTunnel, ResumeTunnel,
		UpdateConfig, CreateRuleSet, UpdateRuleSet, DeleteRuleSet,
		Counter,
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

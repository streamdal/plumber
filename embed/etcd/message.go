package etcd

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	DoFoobarAction = "DoFoobar"
	DoFoobazAction = "DoFoobaz"
)

var (
	ValidActions = []Action{DoFoobarAction, DoFoobazAction}
)

type Action string

type Message struct {
	Action    Action
	Data      []byte // <- consumer decides what's in here based on action
	Metadata  map[string]string
	EmittedBy string
	EmittedAt time.Time // UTC
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

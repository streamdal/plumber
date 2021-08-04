package etcd

import (
	"time"

	"github.com/pkg/errors"
)

var (
	ValidActions = []string{"foobar"}
)

type Message struct {
	Action    int // <- this should be pointing to plumber-schemas messages
	Data      []byte
	Metadata  map[string]string
	EmittedBy string
	EmittedAt time.Time // UTC
}

func (m *Message) Validate() error {
	if m == nil {
		return errors.New("message cannot be nil")
	}

	if m.Action == 0 {
		return errors.New("unrecognized message action")
	}

	if m.EmittedBy == "" {
		return errors.New("EmittedBy cannot be empty")
	}

	if m.EmittedAt.IsZero() {
		return errors.New("EmittedAt cannot be unset")
	}

	return nil
}

package types

import (
	"context"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends"
)

type AttachedStream struct {
	MessageCh chan *records.ReadRecord
}

type Read struct {
	AttachedClientsMutex *sync.RWMutex
	AttachedClients      map[string]*AttachedStream
	PlumberID            string
	ReadOptions          *opts.ReadOptions
	ContextCxl           context.Context
	CancelFunc           context.CancelFunc
	Backend              backends.Backend
	MsgDesc              *desc.MessageDescriptor
	Log                  *logrus.Entry
}

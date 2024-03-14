// Package hostfunc contains host function methods. They are separated from
// go-sdk in order to keep the public API clean.
package hostfunc

import (
	"github.com/pkg/errors"

	"github.com/streamdal/streamdal/sdks/go/kv"
	"github.com/streamdal/streamdal/sdks/go/logger"
)

type HostFunc struct {
	kv  kv.IKV
	log logger.Logger
}

func New(kv kv.IKV, log logger.Logger) (*HostFunc, error) {
	if kv == nil {
		return nil, errors.New("kv cannot be nil")
	}

	return &HostFunc{
		kv:  kv,
		log: log,
	}, nil
}

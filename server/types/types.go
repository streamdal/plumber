package types

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

// Connection is a wrapper around protos.Connect so that we can implement Marshaler interface
type Connection struct {
	*protos.Connection
}

type Schema struct {
	*protos.Schema
}

// Service is a wrapper around protos.Service so that we can implement Marshaler interface
type Service struct {
	*protos.Service
}

// Package helper contains WASM-related helper functions and methods.
// This package is separate from `go-sdk` to avoid polluting
// go-sdk's public API.
package helper

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tetratelabs/wazero/api"
	"google.golang.org/protobuf/proto"
)

// ReadRequestFromMemory is a helper function that reads raw memory starting at
// 'ptr' for 'length' bytes. Once read, it will attempt to unmarshal the data
// into the provided proto.Message.
func ReadRequestFromMemory(module api.Module, msg proto.Message, ptr, length int32) error {
	if length <= 0 {
		return errors.New("length must be greater than 0")
	}

	if module == nil {
		return errors.New("module cannot be nil")
	}

	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	data, ok := module.Memory().Read(uint32(ptr), uint32(length))
	if !ok {
		return errors.New("unable to read memory")
	}

	if err := proto.Unmarshal(data, msg); err != nil {
		return errors.Wrap(err, "unable to unmarshal HttpRequest")
	}

	return nil
}

// WriteResponseToMemory is a helper function that marshals provided message to
// module memory, appends terminators and returns the pointer to the start of
// the message.
func WriteResponseToMemory(module api.Module, msg proto.Message) (uint64, error) {
	if module == nil {
		return 0, errors.New("module cannot be nil")
	}

	if msg == nil {
		return 0, errors.New("msg cannot be nil")
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return 0, errors.Wrap(err, "unable to marshal response")
	}

	alloc := module.ExportedFunction("alloc")
	if alloc == nil {
		return 0, errors.New("unable to get alloc func")
	}

	// Allocate memory for response
	allocRes, err := alloc.Call(context.Background(), uint64(len(data)))
	if err != nil {
		return 0, errors.Wrap(err, "unable to allocate memory")
	}

	if len(allocRes) < 1 {
		return 0, errors.New("alloc returned unexpected number of results")
	}

	// Write memory to allocated space
	ok := module.Memory().Write(uint32(allocRes[0]), data)
	if !ok {
		return 0, errors.New("unable to write host function results to memory")
	}

	return (allocRes[0] << uint64(32)) | uint64(len(data)), nil
}

package server

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/kv"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/util"
)

func (s *Server) ListWasmFiles(_ context.Context, req *protos.ListWasmFilesRequest) (*protos.ListWasmFilesResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	fileNames := make([]string, 0)

	s.PersistentConfig.WasmFilesMutex.RLock()
	for _, file := range s.PersistentConfig.WasmFiles {
		fileNames = append(fileNames, file.Name)
	}
	s.PersistentConfig.WasmFilesMutex.RUnlock()

	return &protos.ListWasmFilesResponse{
		Status: &common.Status{
			Code: common.Code_OK,
		},
		Names: fileNames,
	}, nil
}

func (s *Server) UploadWasmFile(_ context.Context, req *protos.UploadWasmFileRequest) (*protos.UploadWasmFileResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if len(req.Name) == 0 {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "name cannot be empty")
	}

	if len(req.Data) == 0 {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "data cannot be empty")
	}

	zipped, err := util.Compress(req.Data)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, errors.Wrap(err, "unable to compress data").Error())
	}

	// Put data in store
	if err := s.KV.Put(context.Background(), kv.WasmBucket, req.Name, zipped); err != nil {
		return nil, CustomError(common.Code_INTERNAL, errors.Wrap(err, "unable to put data in store").Error())
	}

	// Add to config map
	s.PersistentConfig.WasmFilesMutex.Lock()
	s.PersistentConfig.WasmFiles[req.Name] = &types.WasmFile{
		Name: req.Name,
		// TODO: anything else needed here?
	}
	s.PersistentConfig.WasmFilesMutex.Unlock()

	return &protos.UploadWasmFileResponse{
		Status: &common.Status{
			Code: common.Code_OK,
		},
	}, nil
}

// DownloadWasmFile will return gzipped data, and the client will need to unzip it
func (s *Server) DownloadWasmFile(ctx context.Context, req *protos.DownloadWasmFileRequest) (*protos.DownloadWasmFileResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if req.Name == "" {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "name cannot be empty")
	}

	// Do we know about this file?
	wasmFunc, ok := s.PersistentConfig.WasmFiles[req.Name]
	if !ok {
		return nil, CustomError(common.Code_NOT_FOUND, "wasm file not found")
	}

	// Get data from kv store, we don't keep these in memory due to their potential size
	wasmData, err := s.KV.Get(ctx, kv.WasmBucket, wasmFunc.Name)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	return &protos.DownloadWasmFileResponse{
		Status: &common.Status{
			Code: common.Code_OK,
		},
		Data: wasmData,
	}, nil
}

func (s *Server) DeleteWasmFile(_ context.Context, req *protos.DeleteWasmFileRequest) (*protos.DeleteWasmFileResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if req.Name == "" {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "name cannot be empty")
	}

	// Do we know about this file?
	wasmFunc, ok := s.PersistentConfig.WasmFiles[req.Name]
	if !ok {
		return nil, CustomError(common.Code_NOT_FOUND, "wasm file not found")
	}

	// Delete data from kv store
	if err := s.KV.Delete(context.Background(), kv.WasmBucket, wasmFunc.Name); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	// Delete from map
	s.PersistentConfig.WasmFilesMutex.Lock()
	delete(s.PersistentConfig.WasmFiles, req.Name)
	s.PersistentConfig.WasmFilesMutex.Unlock()

	return &protos.DeleteWasmFileResponse{
		Status: &common.Status{
			Code: common.Code_OK,
		},
	}, nil
}

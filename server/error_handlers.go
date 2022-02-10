package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

// GetLiveErrors ships live errors from plumber that happen in the background
// outside of the normal lifecycle of a gRPC request/response
func (s *Server) GetLiveErrors(req *protos.GetLiveErrorsRequest, srv protos.PlumberServer_GetLiveErrorsServer) error {
	if err := s.validateAuth(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	//id := uuid.NewV4().String()
	//
	//stream := s.ErrorsService.ConnectClient(id)
	//defer s.ErrorsService.DisconnectClient(id)
	//
	//// Ticker is used to periodically send a NOOP response to keep the stream open
	//ticker := time.NewTicker(time.Minute)
	//defer ticker.Stop()
	//
	//go s.testErrors()
	//
	//for {
	//	select {
	//	case msg := <-stream.MessageCh:
	//		srv.Send(&protos.GetLiveErrorsResponse{Error: msg})
	//	case <-ticker.C:
	//		// Send NOOP
	//		s.Log.Info("sent noop")
	//		srv.Send(&protos.GetLiveErrorsResponse{Error: nil})
	//	}
	//}

	return nil
}

func (s *Server) GetErrorHistory(ctx context.Context, req *protos.GetErrorHistoryRequest) (*protos.GetErrorHistoryResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	//history, err := s.ErrorsService.GetHistory(ctx)
	//if err != nil {
	//	return nil, CustomError(common.Code_INTERNAL, err.Error())
	//}
	//
	//return &protos.GetErrorHistoryResponse{
	//	Errors: history,
	//}, nil

	return nil, nil
}

//func (s *Server) testErrors() {
//	ticker := time.NewTicker(time.Second * 90)
//
//	select {
//	case <-ticker.C:
//		s.ErrorsService.AddError(&protos.ErrorMessage{
//			Resource:   "read",
//			ResourceId: uuid.NewV4().String(),
//			Error:      "Some error yo!",
//		})
//	}
//}

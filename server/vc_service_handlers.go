package server

import (
	"fmt"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

// GetVCEvents reads the current stream of VC events from vc-service and sends them to the frontend for consumption
func (s *Server) GetVCEvents(req *protos.GetVCEventsRequest, srv protos.PlumberServer_GetVCEventsServer) error {
	if err := s.validateAuth(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	streamID := uuid.NewV4().String()

	stream := s.VCService.AttachStream(streamID)

	for {
		select {
		case event := <-stream.EventsCh:
			if err := srv.Send(event); err != nil {
				s.Log.Errorf("unable to send vc-service event to frontend: %s", err)
			}
		}
	}

	return nil
}

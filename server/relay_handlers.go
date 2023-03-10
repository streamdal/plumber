package server

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

//func (s *Server) GetAllRelays(_ context.Context, req *protos.GetAllRelaysRequest) (*protos.GetAllRelaysResponse, error) {
//	if err := s.validateAuth(req.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	var numActive int
//	var numInactive int
//
//	RelayOptions := make([]*opts.RelayOptions, 0)
//	for _, v := range s.PersistentConfig.Relays {
//		if v.Active {
//			numActive += 1
//		} else {
//			numInactive += 1
//		}
//
//		v.Options.XActive = v.Active
//		RelayOptions = append(RelayOptions, v.Options)
//	}
//
//	msg := fmt.Sprintf("found '%d' active and '%d' inactive relays", numActive, numInactive)
//
//	return &protos.GetAllRelaysResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			Message:   msg,
//			RequestId: uuid.NewV4().String(),
//		},
//		Opts: RelayOptions,
//	}, nil
//}
//
//func (s *Server) GetRelay(ctx context.Context, request *protos.GetRelayRequest) (*protos.GetRelayResponse, error) {
//	if err := s.validateAuth(request.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	if request.RelayId == "" {
//		return nil, CustomError(common.Code_INVALID_ARGUMENT, "id cannot be empty")
//	}
//
//	relay := s.PersistentConfig.GetRelay(request.RelayId)
//	if relay == nil {
//		return nil, CustomError(common.Code_NOT_FOUND, fmt.Sprintf("relay %s not found", request.RelayId))
//	}
//
//	// We have two active flags because the gRPC response should include whether
//	// the relay is active or not - the method returns a protobuf relay resp
//	// which does not include *relay.
//	relay.Options.XActive = relay.Active
//
//	return &protos.GetRelayResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			RequestId: uuid.NewV4().String(),
//		},
//		Opts: relay.Options,
//	}, nil
//}
//
//func (s *Server) CreateRelay(ctx context.Context, req *protos.CreateRelayRequest) (*protos.CreateRelayResponse, error) {
//	if err := s.validateAuth(req.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	// New relay, create new ID
//	req.Opts.XRelayId = uuid.NewV4().String()
//
//	// This is a new relay so we want actions.CreateRelay to also start it
//	req.Opts.XActive = true
//
//	// Create & start relay
//	r, err := s.Actions.CreateRelay(ctx, req.Opts)
//	if err != nil {
//		s.rollbackCreateRelay(ctx, req.Opts)
//		return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create relay: %s", err))
//	}
//
//	// Publish CreateRelay event
//	// NOTE: For Kafka, if create relay options specify to NOT use a consumer
//	// group, other instances will just ignore the message.
//	//
//	// No consumer group == no load balancing between plumber instances.
//	if err := s.Bus.PublishCreateRelay(ctx, r.Options); err != nil {
//		s.rollbackCreateRelay(ctx, req.Opts)
//		s.Log.Error(err)
//	}
//
//	s.Log.Infof("Relay '%s' started", r.Id)
//
//	return &protos.CreateRelayResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			Message:   "Relay started",
//			RequestId: uuid.NewV4().String(),
//		},
//		RelayId: r.Id,
//	}, nil
//}
//
//func (s *Server) UpdateRelay(ctx context.Context, req *protos.UpdateRelayRequest) (*protos.UpdateRelayResponse, error) {
//	if err := s.validateAuth(req.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	currentRelay := s.PersistentConfig.GetRelay(req.RelayId)
//	if currentRelay.Active {
//		// Publish StopRelay event
//		if err := s.Bus.PublishStopRelay(ctx, currentRelay.Options); err != nil {
//			return nil, fmt.Errorf("unable to publish stop relay event: %s", err)
//		}
//	}
//
//	if _, err := s.Actions.UpdateRelay(ctx, req.RelayId, req.Opts); err != nil {
//		// No need to roll back here since we haven't updated anything yet
//		return nil, CustomError(common.Code_ABORTED, err.Error())
//	}
//
//	if err := s.Bus.PublishUpdateRelay(ctx, req.Opts); err != nil {
//		// TODO: Should have rollback
//		return nil, fmt.Errorf("unable to publish update relay event: %s", err)
//	}
//
//	s.Log.Infof("Relay '%s' updated", req.RelayId)
//
//	return &protos.UpdateRelayResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			Message:   "Relay updated",
//			RequestId: uuid.NewV4().String(),
//		},
//	}, nil
//}
//
//func (s *Server) StopRelay(ctx context.Context, req *protos.StopRelayRequest) (*protos.StopRelayResponse, error) {
//	if err := s.validateAuth(req.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	relay, err := s.Actions.StopRelay(ctx, req.RelayId)
//	if err != nil {
//		if err == validate.ErrRelayNotActive {
//			return nil, CustomError(common.Code_NOT_FOUND, err.Error())
//		}
//
//		if err == validate.ErrRelayNotActive {
//			return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
//		}
//
//		return nil, CustomError(common.Code_ABORTED, err.Error())
//	}
//
//	// Publish StopRelay event
//	if err := s.Bus.PublishStopRelay(ctx, relay.Options); err != nil {
//		fullErr := fmt.Sprintf("unable to publish stop relay event for relay id '%s': %s", relay.Options.XRelayId, err)
//		s.Log.Error(fullErr)
//
//		return nil, CustomError(common.Code_ABORTED, fullErr)
//	}
//
//	s.Log.Infof("Relay '%s' stopped", relay.Id)
//
//	return &protos.StopRelayResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			Message:   "Relay stopped",
//			RequestId: uuid.NewV4().String(),
//		},
//	}, nil
//}
//
//func (s *Server) ResumeRelay(ctx context.Context, req *protos.ResumeRelayRequest) (*protos.ResumeRelayResponse, error) {
//	if err := s.validateAuth(req.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	relay, err := s.Actions.ResumeRelay(ctx, req.RelayId)
//	if err != nil {
//		if err == validate.ErrRelayNotFound {
//			return nil, CustomError(common.Code_NOT_FOUND, err.Error())
//		}
//
//		if err == validate.ErrRelayAlreadyActive {
//			return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
//		}
//
//		return nil, CustomError(common.Code_ABORTED, err.Error())
//	}
//
//	// Publish ResumeRelay event
//	if err := s.Bus.PublishResumeRelay(ctx, relay.Options); err != nil {
//		fullErr := fmt.Sprintf("unable to publish resume relay event for relay id '%s': %s", relay.Options.XRelayId, err)
//		s.Log.Error(fullErr)
//
//		return nil, CustomError(common.Code_ABORTED, fullErr)
//	}
//
//	s.Log.Infof("Relay '%s' resumed", relay.Id)
//
//	return &protos.ResumeRelayResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			Message:   "Relay resumed",
//			RequestId: uuid.NewV4().String(),
//		},
//	}, nil
//}
//
//func (s *Server) DeleteRelay(ctx context.Context, req *protos.DeleteRelayRequest) (*protos.DeleteRelayResponse, error) {
//	if err := s.validateAuth(req.Auth); err != nil {
//		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
//	}
//
//	relay, err := s.Actions.DeleteRelay(ctx, req.RelayId)
//	if err != nil {
//		if err == validate.ErrRelayNotFound {
//			return nil, CustomError(common.Code_NOT_FOUND, err.Error())
//		}
//
//		return nil, CustomError(common.Code_ABORTED, err.Error())
//	}
//
//	// Publish delete event
//	if err := s.Bus.PublishDeleteRelay(ctx, relay.Options); err != nil {
//		s.Log.Error(err)
//	}
//
//	s.Log.Infof("Relay '%s' deleted", relay.Id)
//
//	return &protos.DeleteRelayResponse{
//		Status: &common.Status{
//			Code:      common.Code_OK,
//			Message:   "Relay deleted",
//			RequestId: uuid.NewV4().String(),
//		},
//	}, nil
//}

// TODO: Implement
func (s *Server) rollbackUpdateRelay(ctx context.Context, relayOptions *opts.RelayOptions) {
	// Stop grpc relayers

	// Remove from persistent storage
}

func (s *Server) rollbackCreateRelay(ctx context.Context, req *opts.RelayOptions) {
	// TODO: Implement
}

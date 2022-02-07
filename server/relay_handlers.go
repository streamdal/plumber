package server

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/validate"
)

func (s *Server) GetAllRelays(_ context.Context, req *protos.GetAllRelaysRequest) (*protos.GetAllRelaysResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	var numActive int
	var numInactive int

	RelayOptions := make([]*opts.RelayOptions, 0)
	for _, v := range s.PersistentConfig.Relays {
		if v.Active {
			numActive += 1
		} else {
			numInactive += 1
		}

		v.Options.XActive = v.Active
		RelayOptions = append(RelayOptions, v.Options)
	}

	msg := fmt.Sprintf("found '%d' active and '%d' inactive relays", numActive, numInactive)

	return &protos.GetAllRelaysResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   msg,
			RequestId: uuid.NewV4().String(),
		},
		Opts: RelayOptions,
	}, nil
}

func (s *Server) GetRelay(ctx context.Context, request *protos.GetRelayRequest) (*protos.GetRelayResponse, error) {
	if err := s.validateAuth(request.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if request.RelayId == "" {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "id cannot be empty")
	}

	relay := s.PersistentConfig.GetRelay(request.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, fmt.Sprintf("relay %s not found", request.RelayId))
	}

	// TODO: figure out why we have two active flags and explain here in a comment
	relay.Options.XActive = relay.Active

	return &protos.GetRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
		Opts: relay.Options,
	}, nil
}

func (s *Server) CreateRelay(ctx context.Context, req *protos.CreateRelayRequest) (*protos.CreateRelayResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// New relay, create new ID
	req.Opts.XRelayId = uuid.NewV4().String()

	// This is a new relay so we want actions.CreateRelay to also start it
	req.Opts.XActive = true

	// Create & start relay
	r, err := s.Actions.CreateRelay(ctx, req.Opts)
	if err != nil {
		s.rollbackCreateRelay(ctx, req.Opts)
		return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create relay: %s", err))
	}

	// Save to etcd
	data, err := proto.Marshal(req.Opts)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal relay")
	}

	_, err = s.Etcd.Put(ctx, etcd.CacheRelaysPrefix+"/"+req.Opts.XRelayId, string(data))
	if err != nil {
		s.rollbackCreateRelay(ctx, req.Opts)
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateRelay event
	// NOTE: For Kafka, if create relay options specify to NOT use a consumer
	// group, other instances will just ignore the message.
	//
	// No consumer group == no load balancing between plumber instances.
	if err := s.Etcd.PublishCreateRelay(ctx, r.Options); err != nil {
		s.rollbackCreateRelay(ctx, req.Opts)
		s.Log.Error(err)
	}

	s.Log.Infof("Relay '%s' started", r.Id)

	return &protos.CreateRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay started",
			RequestId: uuid.NewV4().String(),
		},
		RelayId: r.Id,
	}, nil
}

// TODO: Needs to be updated similar to CreateRelay
func (s *Server) UpdateRelay(_ context.Context, req *protos.UpdateRelayRequest) (*protos.UpdateRelayResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Stop existing relay
	relay := s.PersistentConfig.GetRelay(req.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	}

	s.Log.Infof("Stopping relay '%s'", relay.Id)

	// Stop workers
	relay.CancelFunc()

	// TODO: need to get signal of when relay shutdown is complete
	time.Sleep(time.Second)

	// Update relay config
	relay.Options = req.Opts

	// Give relayer new ctx & cancel func
	ctx, cancelFunc := context.WithCancel(context.Background())

	relay.CancelCtx = ctx
	relay.CancelFunc = cancelFunc

	if err := relay.StartRelay(5 * time.Second); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	data, err := proto.Marshal(relay.Options)
	if err != nil {
		s.rollbackUpdateRelay(ctx, req.Opts)
		fullErr := fmt.Sprintf("unable to marshal relay options to bytes for relay id '%s': %s", req.Opts.XRelayId, err)
		s.Log.Error(fullErr)

		return nil, CustomError(common.Code_ABORTED, fullErr)
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, etcd.CacheRelaysPrefix+"/"+req.Opts.XRelayId, string(data))
	if err != nil {
		s.rollbackUpdateRelay(ctx, req.Opts)
		fullErr := fmt.Sprintf("unable to save new relay options to etcd for relay id '%s': %s", req.Opts.XRelayId, err)
		s.Log.Error(fullErr)

		return nil, CustomError(common.Code_ABORTED, fullErr)
	}

	// Save to memory
	s.PersistentConfig.SetRelay(relay.Id, relay)

	// Publish CreateSchema event
	if err := s.Etcd.PublishUpdateRelay(ctx, relay.Options); err != nil {
		s.rollbackUpdateRelay(ctx, req.Opts)
		fullErr := fmt.Sprintf("unable to publish update relay event for relay id '%s': %s", req.Opts.XRelayId, err)
		s.Log.Error(fullErr)

		return nil, CustomError(common.Code_ABORTED, fullErr)
	}

	s.Log.Infof("Relay '%s' updated", relay.Id)

	relay.Active = true

	return &protos.UpdateRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) StopRelay(ctx context.Context, req *protos.StopRelayRequest) (*protos.StopRelayResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay, err := s.Actions.StopRelay(ctx, req.RelayId)
	if err != nil {
		if err == validate.ErrRelayNotActive {
			return nil, CustomError(common.Code_NOT_FOUND, err.Error())
		}

		if err == validate.ErrRelayNotActive {
			return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
		}

		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save new state in etcd
	data, err := proto.Marshal(relay.Options)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal updated relay opts")
	}

	_, err = s.Etcd.Put(ctx, etcd.CacheRelaysPrefix+"/"+relay.Options.XRelayId, string(data))
	if err != nil {
		s.rollbackCreateRelay(ctx, relay.Options)
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish StopRelay event
	if err := s.Etcd.PublishStopRelay(ctx, relay.Options); err != nil {
		fullErr := fmt.Sprintf("unable to publish stop relay event for relay id '%s': %s", relay.Options.XRelayId, err)
		s.Log.Error(fullErr)

		return nil, CustomError(common.Code_ABORTED, fullErr)
	}

	s.Log.Infof("Relay '%s' stopped", relay.Id)

	return &protos.StopRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay stopped",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) ResumeRelay(ctx context.Context, req *protos.ResumeRelayRequest) (*protos.ResumeRelayResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay, err := s.Actions.ResumeRelay(ctx, req.RelayId)
	if err != nil {
		if err == validate.ErrRelayNotFound {
			return nil, CustomError(common.Code_NOT_FOUND, err.Error())
		}

		if err == validate.ErrRelayAlreadyActive {
			return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
		}

		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save new state in etcd
	data, err := proto.Marshal(relay.Options)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal updated relay opts")
	}

	_, err = s.Etcd.Put(ctx, etcd.CacheRelaysPrefix+"/"+relay.Options.XRelayId, string(data))
	if err != nil {
		s.rollbackCreateRelay(ctx, relay.Options)
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish ResumeRelay event
	if err := s.Etcd.PublishResumeRelay(ctx, relay.Options); err != nil {
		fullErr := fmt.Sprintf("unable to publish resume relay event for relay id '%s': %s", relay.Options.XRelayId, err)
		s.Log.Error(fullErr)

		return nil, CustomError(common.Code_ABORTED, fullErr)
	}

	s.Log.Infof("Relay '%s' resumed", relay.Id)

	return &protos.ResumeRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay resumed",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) DeleteRelay(ctx context.Context, req *protos.DeleteRelayRequest) (*protos.DeleteRelayResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay, err := s.Actions.DeleteRelay(ctx, req.RelayId)
	if err != nil {
		if err == validate.ErrRelayNotFound {
			return nil, CustomError(common.Code_NOT_FOUND, err.Error())
		}

		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Delete in etcd
	if _, err := s.Etcd.Delete(ctx, etcd.CacheRelaysPrefix+"/"+relay.Id); err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete relay in etcd: "+err.Error()))
	}

	// Publish delete event
	if err := s.Etcd.PublishDeleteRelay(ctx, relay.Options); err != nil {
		s.Log.Error(err)
	}

	return &protos.DeleteRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

// TODO: Implement
func (s *Server) rollbackUpdateRelay(ctx context.Context, relayOptions *opts.RelayOptions) {
	// Remove from etcd

	// Stop grpc relayers

	// Remove from persistent storage
}

func (s *Server) rollbackCreateRelay(ctx context.Context, req *opts.RelayOptions) {
	// TODO: Implement
}

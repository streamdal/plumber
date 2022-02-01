package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (s *Server) GetAllDynamic(_ context.Context, req *protos.GetAllDynamicRequest) (*protos.GetAllDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil

	//RelayOptions := make([]*opts.RelayOptions, 0)
	//for _, v := range s.PersistentConfig.Relays {
	//	RelayOptions = append(RelayOptions, v.Options)
	//}
	//
	//return &protos.GetAllRelaysResponse{
	//	Status: &common.Status{
	//		Code:      common.Code_OK,
	//		RequestId: uuid.NewV4().String(),
	//	},
	//	Opts: RelayOptions,
	//}, nil
}

func (s *Server) GetDynamic(ctx context.Context, request *protos.GetDynamicRequest) (*protos.GetDynamicResponse, error) {
	panic("implement me")
}

// TODO: Implement
func (s *Server) CreateDynamic(ctx context.Context, req *protos.CreateDynamicRequest) (*protos.CreateDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	panic("implement me")

	//if err := validate.DynamicOptionsForServer(req.Opts); err != nil {
	//	return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to validate dynamic options: %s", err))
	//}
	//
	//// Get stored connection information
	//conn := s.PersistentConfig.GetConnection(req.Opts.ConnectionId)
	//if conn == nil {
	//	return nil, CustomError(common.Code_NOT_FOUND, validate.ErrConnectionNotFound.Error())
	//}
	//
	//// Try to create a backend from given connection options
	//be, err := backends.New(conn)
	//if err != nil {
	//	return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create backend: %s", err))
	//}
	//
	//// Save to etcd
	//data, err := proto.Marshal(req.Opts)
	//if err != nil {
	//	return nil, CustomError(common.Code_ABORTED, "could not marshal relay")
	//}
	//
	//_, err = s.Etcd.Put(ctx, etcd.CacheDynamicPrefix+"/"+req.Opts.XDynamicId, string(data))
	//if err != nil {
	//	s.rollbackCreateDynamic(ctx, req.Opts)
	//	return nil, CustomError(common.Code_ABORTED, err.Error())
	//}
	//
	//// Used to shutdown dynamic destinations on StopDynamic() gRPC call
	//shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	//
	//d := &types.Dynamic{
	//	Id:         uuid.NewV4().String(),
	//	Backend:    be,
	//	CancelFunc: shutdownFunc,
	//	CancelCtx:  shutdownCtx,
	//	Options:    req.Opts,
	//}
	//
	//// Save to memory
	//s.PersistentConfig.SetDynamic(d.Id, d)
	//
	//// Publish CreateSchema event
	//if err := s.Etcd.PublishCreateDynamic(ctx, d.Options); err != nil {
	//	s.rollbackCreateDynamic(ctx, req.Opts)
	//	s.Log.Error(err)
	//}
	//
	//// Start the dynamic tunnel
	//if err := d.StartDynamic(); err != nil {
	//	s.rollbackCreateDynamic(ctx, req.Opts)
	//	return nil, errors.Wrap(err, "unable to start relay")
	//}
	//
	//d.Options.XRelayId = d.Id
	//d.Active = true
	//
	//s.Log.Infof("Replay tunnel '%s' started", d.Id)
	//
	//return &protos.CreateDynamicResponse{
	//	Status: &common.Status{
	//		Code:      common.Code_OK,
	//		Message:   "Dynamic tunnel created",
	//		RequestId: uuid.NewV4().String(),
	//	},
	//	DynamicId: d.Id,
	//}, nil
}

func (s *Server) rollbackCreateDynamic(ctx context.Context, req *opts.DynamicOptions) {
	// TODO: Implement
}

// TODO: Implement
func (s *Server) UpdateDynamic(_ context.Context, req *protos.UpdateDynamicRequest) (*protos.UpdateDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil

	//// Stop existing relay
	//relay := s.PersistentConfig.GetRelay(req.RelayId)
	//if relay == nil {
	//	return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	//}
	//
	//// Stop workers
	//relay.CancelFunc()
	//
	//s.Log.Infof("Relay '%s' stopped", relay.Id)
	//
	//// TODO: need to get signal of when relay shutdown is complete
	//time.Sleep(time.Second)
	//
	//// Update relay config
	//relay.Options = req.Opts
	//
	//// Give relayer new ctx & cancel func
	//ctx, cancelFunc := context.WithCancel(context.Background())
	//
	//relay.CancelCtx = ctx
	//relay.CancelFunc = cancelFunc
	//
	//if err := relay.StartRelay(); err != nil {
	//	return nil, errors.Wrap(err, "unable to start relay")
	//}
	//
	//data, err := proto.Marshal(relay.Options)
	//if err != nil {
	//	s.rollbackUpdateRelay(ctx, req.Opts)
	//	fullErr := fmt.Sprintf("unable to marshal relay options to bytes for relay id '%s': %s", req.Opts.XRelayId, err)
	//	s.Log.Error(fullErr)
	//
	//	return nil, CustomError(common.Code_ABORTED, fullErr)
	//}
	//
	//// Save to etcd
	//_, err = s.Etcd.Put(ctx, etcd.CacheRelaysPrefix+"/"+req.Opts.XRelayId, string(data))
	//if err != nil {
	//	s.rollbackUpdateRelay(ctx, req.Opts)
	//	fullErr := fmt.Sprintf("unable to save new relay options to etcd for relay id '%s': %s", req.Opts.XRelayId, err)
	//	s.Log.Error(fullErr)
	//
	//	return nil, CustomError(common.Code_ABORTED, fullErr)
	//}
	//
	//// Save to memory
	//s.PersistentConfig.SetRelay(relay.Id, relay)
	//
	//// Publish CreateSchema event
	//if err := s.Etcd.PublishUpdateRelay(ctx, relay.Options); err != nil {
	//	s.rollbackUpdateRelay(ctx, req.Opts)
	//	fullErr := fmt.Sprintf("unable to publish update relay event for relay id '%s': %s", req.Opts.XRelayId, err)
	//	s.Log.Error(fullErr)
	//
	//	return nil, CustomError(common.Code_ABORTED, fullErr)
	//}
	//
	//s.Log.Infof("Relay '%s' started", relay.Id)
	//
	//relay.Active = true
	//
	//return &protos.UpdateRelayResponse{
	//	Status: &common.Status{
	//		Code:      common.Code_OK,
	//		Message:   "Relay updated",
	//		RequestId: uuid.NewV4().String(),
	//	},
	//}, nil
}

// TODO: Implement
func (s *Server) rollbackUpdateDynamic(ctx context.Context, dynamicOptions *opts.DynamicOptions) {
	// Remove from etcd

	// Stop dynamic destination

	// Remove from persistent storage
}

func (s *Server) StopDynamic(_ context.Context, req *protos.StopDynamicRequest) (*protos.StopDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil

	//relay := s.PersistentConfig.GetRelay(req.RelayId)
	//if relay == nil {
	//	return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	//}
	//
	//if !relay.Active {
	//	return nil, CustomError(common.Code_FAILED_PRECONDITION, "Relay is not active")
	//}
	//
	//// Stop workers
	//relay.CancelFunc()
	//relay.Active = false
	//
	//s.Log.Infof("Relay '%s' stopped", relay.Id)
	//
	//return &protos.StopRelayResponse{
	//	Status: &common.Status{
	//		Code:      common.Code_OK,
	//		Message:   "Relay stopped",
	//		RequestId: uuid.NewV4().String(),
	//	},
	//}, nil
}

// TODO: Implement
func (s *Server) ResumeDynamic(ctx context.Context, req *protos.ResumeDynamicRequest) (*protos.ResumeDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil

	//relay := s.PersistentConfig.GetRelay(req.RelayId)
	//if relay == nil {
	//	return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	//}
	//
	//if relay.Active {
	//	return nil, CustomError(common.Code_FAILED_PRECONDITION, "Relay is not stopped")
	//}
	//
	//ctx, cancelFunc := context.WithCancel(context.Background())
	//relay.CancelCtx = ctx
	//relay.CancelFunc = cancelFunc
	//
	//if err := relay.StartRelay(); err != nil {
	//	return nil, errors.Wrap(err, "unable to start relay")
	//}
	//
	//s.Log.Infof("Relay '%s' started", relay.Id)
	//
	//relay.Active = true
	//
	//return &protos.ResumeRelayResponse{
	//	Status: &common.Status{
	//		Code:      common.Code_OK,
	//		Message:   "Relay resumed",
	//		RequestId: uuid.NewV4().String(),
	//	},
	//}, nil
}

// TODO: Implement
func (s *Server) DeleteDynamic(ctx context.Context, req *protos.DeleteDynamicRequest) (*protos.DeleteDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return nil, nil

	//if err := s.validateAuth(req.Auth); err != nil {
	//	return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	//}
	//
	//relay := s.PersistentConfig.GetRelay(req.RelayId)
	//if relay == nil {
	//	return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	//}
	//
	//// Stop workers
	//relay.CancelFunc()
	//
	//// Delete in etcd
	//_, err := s.Etcd.Delete(ctx, etcd.CacheRelaysPrefix+"/"+relay.Id)
	//if err != nil {
	//	return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete connection: "+err.Error()))
	//}
	//
	//// Delete in memory
	//s.PersistentConfig.DeleteRelay(relay.Id)
	//
	//// Publish delete event
	//if err := s.Etcd.PublishDeleteRelay(ctx, relay.Options); err != nil {
	//	s.Log.Error(err)
	//}
	//
	//return &protos.DeleteDynamicResponse{
	//	Status: &common.Status{
	//		Code:      common.Code_OK,
	//		Message:   "Relay deleted",
	//		RequestId: uuid.NewV4().String(),
	//	},
	//}, nil
}

package server

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (s *Server) GetAllConnections(_ context.Context, req *protos.GetAllConnectionsRequest) (*protos.GetAllConnectionsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conns := make([]*opts.ConnectionOptions, 0)
	for _, v := range s.PersistentConfig.Connections {
		conns = append(conns, v.Connection)
	}

	return &protos.GetAllConnectionsResponse{
		Options: conns,
	}, nil
}

func (s *Server) GetConnection(_ context.Context, req *protos.GetConnectionRequest) (*protos.GetConnectionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conn := s.PersistentConfig.GetConnection(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	return &protos.GetConnectionResponse{
		Options: conn.Connection,
	}, nil
}

func (s *Server) CreateConnection(ctx context.Context, req *protos.CreateConnectionRequest) (*protos.CreateConnectionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	connOpts := req.GetOptions()
	if connOpts == nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "No connection message found in the payload")
	}
	connOpts.XId = uuid.NewV4().String()

	if err := validate.ConnectionOptionsForServer(connOpts); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	// Save connection options in mem
	s.PersistentConfig.SetConnection(connOpts.XId, &types.Connection{Connection: connOpts})
	s.PersistentConfig.Save()

	// TODO: What if the publish fails - how do other nodes know about the new
	// connection? Once this is figured out, we can move this down; for now,
	// this should fail the request. ~ds
	if err := s.Bus.PublishCreateConnection(ctx, connOpts); err != nil {
		s.rollbackCreateConnection(ctx, connOpts)

		s.Log.Error(errors.Wrap(err, "unable to publish create connection event"))
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to create connection event: %s", err))
	}

	s.Log.Infof("Connection '%s' created", connOpts.XId)

	return &protos.CreateConnectionResponse{
		ConnectionId: connOpts.XId,
	}, nil
}

// Rollback anything that may have been done during a conn creation request
func (s *Server) rollbackCreateConnection(ctx context.Context, connOpts *opts.ConnectionOptions) {
	// Delete connections options map entry
	s.PersistentConfig.DeleteConnection(connOpts.XId)
	s.PersistentConfig.Save()
}

func (s *Server) TestConnection(ctx context.Context, req *protos.TestConnectionRequest) (*protos.TestConnectionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if err := validate.ConnectionOptionsForServer(req.Options); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	// Fetch the associated backend
	conn := s.PersistentConfig.GetConnection(req.Options.XId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "unable to find backend for given connection id")
	}

	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create backend: %s", err))
	}

	if err := be.Test(ctx); err != nil {
		return &protos.TestConnectionResponse{
			Status: &common.Status{
				Code:      common.Code_INTERNAL,
				Message:   err.Error(),
				RequestId: uuid.NewV4().String(),
			},
		}, nil
	}

	return &protos.TestConnectionResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Connected successfully",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) UpdateConnection(ctx context.Context, req *protos.UpdateConnectionRequest) (*protos.UpdateConnectionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	conn := s.PersistentConfig.GetConnection(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	if err := validate.ConnectionOptionsForServer(req.Options); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	// Re-assign connection options so we can update in-mem + etcd
	conn.Connection = req.Options

	// Update conf
	s.PersistentConfig.SetConnection(conn.Connection.XId, conn)
	s.PersistentConfig.Save()

	//Publish UpdateConnection event
	if err := s.Bus.PublishUpdateConnection(ctx, conn.Connection); err != nil {
		s.Log.Error(err)
	}

	s.Log.WithField("request_id", requestID).Infof("Connection '%s' updated", req.ConnectionId)

	return &protos.UpdateConnectionResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Connection updated",
			RequestId: requestID,
		},
	}, nil
}

func (s *Server) DeleteConnection(ctx context.Context, req *protos.DeleteConnectionRequest) (*protos.DeleteConnectionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	conn := s.PersistentConfig.GetConnection(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	// Ensure this connection isn't being used by any tunnels
	s.PersistentConfig.TunnelsMutex.RLock()
	for id, tunnel := range s.PersistentConfig.Tunnels {
		if tunnel.Options.ConnectionId == id {
			s.PersistentConfig.TunnelsMutex.RUnlock()
			return nil, fmt.Errorf("cannot delete connection '%s' because it is in use by tunnel '%s'",
				id, tunnel.Options.XTunnelId)
		}
	}
	s.PersistentConfig.TunnelsMutex.RUnlock()

	// Ensure this connection isn't being used by any relays
	s.PersistentConfig.RelaysMutex.RLock()
	for id, relay := range s.PersistentConfig.Relays {
		if relay.Options.ConnectionId == id {
			s.PersistentConfig.RelaysMutex.RUnlock()
			return nil, fmt.Errorf("cannot delete connection '%s' because it is in use by relay '%s'",
				id, relay.Options.XRelayId)
		}
	}
	s.PersistentConfig.RelaysMutex.RUnlock()

	// Delete in memory
	s.PersistentConfig.DeleteConnection(conn.Connection.XId)
	s.PersistentConfig.Save()

	// Publish DeleteConnection event
	if err := s.Bus.PublishDeleteConnection(ctx, conn.Connection); err != nil {
		s.Log.Error(err)
	}

	s.Log.WithField("request_id", requestID).Infof("Connection '%s' deleted", req.ConnectionId)

	return &protos.DeleteConnectionResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Connection deleted",
			RequestId: requestID,
		},
	}, nil
}

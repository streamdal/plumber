package server

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/server/types"
)

func (p *PlumberServer) GetAllConnections(_ context.Context, req *protos.GetAllConnectionsRequest) (*protos.GetAllConnectionsResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conns := make([]*protos.Connection, 0)
	for _, v := range p.PersistentConfig.Connections {
		conns = append(conns, v.Connection)
	}

	return &protos.GetAllConnectionsResponse{
		Connections: conns,
	}, nil
}

func (p *PlumberServer) GetConnection(_ context.Context, req *protos.GetConnectionRequest) (*protos.GetConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conn := p.PersistentConfig.GetConnection(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	return &protos.GetConnectionResponse{
		Connection: conn.Connection,
	}, nil
}

func (p *PlumberServer) CreateConnection(ctx context.Context, req *protos.CreateConnectionRequest) (*protos.CreateConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conn := req.GetConnection()
	if conn == nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "No connection message found in the payload")
	}
	conn.Id = uuid.NewV4().String()

	if err := validateConnection(req.GetConnection()); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	data, err := proto.Marshal(conn)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal connection")
	}

	// Save to etcd
	_, err = p.Etcd.Put(ctx, EtcdConnectionsPrefix+"/"+conn.Id, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	p.PersistentConfig.SetConnection(conn.Id, &types.Connection{Connection: conn})

	if err := p.Etcd.PublishCreateConnection(ctx, conn); err != nil {
		p.Log.Error(err)
	}

	p.Log.Infof("Connection '%s' created", conn.Id)

	return &protos.CreateConnectionResponse{
		ConnectionId: conn.Id,
	}, nil
}

func (p *PlumberServer) TestConnection(_ context.Context, req *protos.TestConnectionRequest) (*protos.TestConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if err := validateConnection(req.GetConnection()); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	if err := testConnection(req.GetConnection()); err != nil {
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

func (p *PlumberServer) UpdateConnection(ctx context.Context, req *protos.UpdateConnectionRequest) (*protos.UpdateConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	conn := p.PersistentConfig.GetConnection(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	if err := validateConnection(req.GetConnection()); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	conn.Connection = req.Connection

	data, err := proto.Marshal(conn.Connection)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal connection")
	}

	// Update in etcd
	_, err = p.Etcd.Put(ctx, EtcdConnectionsPrefix+"/"+conn.Id, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Update in memory
	p.PersistentConfig.SetConnection(req.Connection.Id, &types.Connection{Connection: req.Connection})

	// Publish UpdateConnection event
	if err := p.Etcd.PublishUpdateConnection(ctx, conn.Connection); err != nil {
		p.Log.Error(err)
	}

	p.Log.WithField("request_id", requestID).Infof("Connection '%s' updated", req.ConnectionId)

	return &protos.UpdateConnectionResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Connection updated",
			RequestId: requestID,
		},
	}, nil
}

func (p *PlumberServer) DeleteConnection(ctx context.Context, req *protos.DeleteConnectionRequest) (*protos.DeleteConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	conn := p.PersistentConfig.GetConnection(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	// Delete in etcd
	_, err := p.Etcd.Delete(ctx, EtcdConnectionsPrefix+"/"+conn.Id)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete connection: "+err.Error()))
	}

	// Delete in memory
	p.PersistentConfig.DeleteConnection(conn.Id)

	// Publish DeleteConnection event
	if err := p.Etcd.PublishDeleteConnection(ctx, conn.Connection); err != nil {
		p.Log.Error(err)
	}

	p.Log.WithField("request_id", requestID).Infof("Connection '%s' deleted", req.ConnectionId)

	return &protos.DeleteConnectionResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Connection deleted",
			RequestId: requestID,
		},
	}, nil
}

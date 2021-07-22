package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

var (
	ErrMissingConnection = errors.New("connection cannot be nil")
	ErrMissingAddress    = errors.New("at least one kafka server address must be specified")
	ErrMissingUsername   = errors.New("you must provide a username when specifying a SASL type")
	ErrMissingPassword   = errors.New("you must provide a password when specifying a SASL type")
)

// setConn sets in-memory connection
func (p *PlumberServer) setConn(conn *protos.Connection) {
	p.ConnectionsMutex.Lock()
	p.PersistentConfig.Connections[conn.Id] = conn
	p.ConnectionsMutex.Unlock()

	if err := p.PersistentConfig.Save(); err != nil {
		p.Log.Error(err)
	}
}

// getConn retrieves in memory connection
func (p *PlumberServer) getConn(connID string) *protos.Connection {
	p.ConnectionsMutex.RLock()
	defer p.ConnectionsMutex.RUnlock()
	return p.PersistentConfig.Connections[connID]
}

func (p *PlumberServer) GetAllConnections(_ context.Context, req *protos.GetAllConnectionsRequest) (*protos.GetAllConnectionsResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conns := make([]*protos.Connection, 0)
	for _, v := range p.PersistentConfig.Connections {
		conns = append(conns, v)
	}

	return &protos.GetAllConnectionsResponse{
		Connections: conns,
	}, nil
}

func (p *PlumberServer) GetConnection(_ context.Context, req *protos.GetConnectionRequest) (*protos.GetConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conn := p.getConn(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	return &protos.GetConnectionResponse{
		Connection: conn,
	}, nil
}

func (p *PlumberServer) CreateConnection(_ context.Context, req *protos.CreateConnectionRequest) (*protos.CreateConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	conn := req.GetConnection()
	conn.Id = uuid.NewV4().String()

	if err := validateConnection(req.GetConnection()); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	p.setConn(req.Connection)

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

	// TODO

	return nil, nil
}

func (p *PlumberServer) UpdateConnection(_ context.Context, req *protos.UpdateConnectionRequest) (*protos.UpdateConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	conn := p.getConn(req.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	if err := validateConnection(req.GetConnection()); err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	p.setConn(req.Connection)

	p.Log.WithField("request_id", requestID).Infof("Connection '%s' updated", req.ConnectionId)

	return &protos.UpdateConnectionResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Connection updated",
			RequestId: requestID,
		},
	}, nil
}

func (p *PlumberServer) DeleteConnection(_ context.Context, req *protos.DeleteConnectionRequest) (*protos.DeleteConnectionResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	p.ConnectionsMutex.Lock()
	defer p.ConnectionsMutex.Unlock()
	_, ok := p.PersistentConfig.Connections[req.ConnectionId]
	if !ok {
		return nil, CustomError(common.Code_NOT_FOUND, "no such connection id")
	}

	delete(p.PersistentConfig.Connections, req.ConnectionId)

	if err := p.PersistentConfig.Save(); err != nil {
		p.Log.Errorf("unable to save updated connections list: %s", err)
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

// validateConnection ensures all required parameters are passed when creating/testing/updating a connection
func validateConnection(conn *protos.Connection) error {
	if conn == nil {
		return ErrMissingConnection
	}

	switch {
	case conn.GetKafka() != nil:
		return validateConnectionKafka(conn.GetKafka())
	}

	return nil
}

// validateConnectionKafka ensures all required parameters are passed when creating/testing/updating a kafka connection
func validateConnectionKafka(conn *conns.Kafka) error {
	if len(conn.Address) == 0 {
		return ErrMissingAddress
	}

	if conn.SaslType != conns.SASLType_NONE && conn.SaslUsername == "" {
		return ErrMissingUsername
	}

	if conn.SaslType != conns.SASLType_NONE && conn.SaslPassword == "" {
		return ErrMissingPassword
	}

	return nil
}

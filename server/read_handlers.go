package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
)

func (s *Server) GetAllReads(_ context.Context, req *protos.GetAllReadsRequest) (*protos.GetAllReadsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	allReadOptions := make([]*opts.ReadOptions, 0)

	for _, v := range s.PersistentConfig.Reads {
		allReadOptions = append(allReadOptions, v.ReadOptions)
	}

	return &protos.GetAllReadsResponse{
		Read: allReadOptions,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) StartRead(req *protos.StartReadRequest, srv protos.PlumberServer_StartReadServer) error {
	requestID := uuid.NewV4().String()

	if err := s.validateAuth(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if req.ReadId == "" {
		return CustomError(common.Code_FAILED_PRECONDITION, "read not found")
	}

	read := s.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return CustomError(common.Code_NOT_FOUND, "read not found")
	}

	stream := &types.AttachedStream{
		MessageCh: make(chan *records.ReadRecord, 1000),
	}

	read.AttachedClientsMutex.Lock()
	read.AttachedClients[requestID] = stream
	read.AttachedClientsMutex.Unlock()

	llog := s.Log.WithField("read_id", read.ReadOptions.XId).
		WithField("client_id", requestID)

	// Ensure we remove this client from the active streams on exit
	defer func() {
		read.AttachedClientsMutex.Lock()
		delete(read.AttachedClients, requestID)
		read.AttachedClientsMutex.Unlock()
		llog.Debugf("Stream detached for '%s'", requestID)
	}()

	llog.Debugf("New stream attached")

	ticker := time.NewTicker(time.Minute)

	// Start reading
	for {
		select {
		case <-ticker.C:
			// NOOP to keep connection open
			res := &protos.StartReadResponse{
				Status: &common.Status{
					Code:      common.Code_OK,
					Message:   "keep alive",
					RequestId: requestID,
				},
			}
			if err := srv.Send(res); err != nil {
				llog.Error(err)
				continue
			}
			llog.Debug("Sent keepalive")

		case msg := <-stream.MessageCh:
			messages := make([]*records.ReadRecord, 0)

			// TODO: batch these up and send multiple per response?
			messages = append(messages, msg)

			res := &protos.StartReadResponse{
				Status: &common.Status{
					Code:      common.Code_OK,
					Message:   "Message read",
					RequestId: requestID,
				},
				Records: messages,
			}
			if err := srv.Send(res); err != nil {
				llog.Error(err)
				continue
			}
			llog.Debugf("Sent message to client '%s'", requestID)
		case <-read.ContextCxl.Done():
			// StartRead stopped. close out all streams for it
			llog.Debugf("Read stopped. closing stream for client '%s'", requestID)
			return nil
		default:
			// NOOP
		}
	}
}

func (s *Server) populateDecodeSchemaDetails(read *opts.ReadOptions) error {
	if read.DecodeOptions == nil {
		return nil
	}

	schemaID := read.DecodeOptions.SchemaId
	if schemaID == "" {
		return nil
	}

	cachedSchemaOptions := s.PersistentConfig.GetSchema(schemaID)
	if cachedSchemaOptions == nil {
		return fmt.Errorf("schema '%s' not found", schemaID)
	}

	versions := cachedSchemaOptions.GetVersions()
	latestSchema := versions[len(versions)-1]

	switch read.DecodeOptions.DecodeType {
	case encoding.DecodeType_DECODE_TYPE_PROTOBUF:
		// Set the entire struct, since it probably won't be passed if just a schema ID is passed
		read.DecodeOptions.ProtobufSettings = &encoding.ProtobufSettings{
			ProtobufRootMessage: latestSchema.GetProtobufSettings().ProtobufRootMessage,
			XMessageDescriptor:  latestSchema.GetProtobufSettings().XMessageDescriptor,
		}
	case encoding.DecodeType_DECODE_TYPE_AVRO:
		// Set the entire struct, since it probably won't be passed if just a schema ID is passed
		read.DecodeOptions.AvroSettings = &encoding.AvroSettings{
			AvroSchemaFile: latestSchema.GetAvroSettings().AvroSchemaFile,
			Schema:         latestSchema.GetAvroSettings().Schema,
		}
	case encoding.DecodeType_DECODE_TYPE_THRIFT:
		// TODO: implement eventually
	}

	return nil
}

func (s *Server) CreateRead(_ context.Context, req *protos.CreateReadRequest) (*protos.CreateReadResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if err := validate.ReadOptionsForServer(req.Read); err != nil {
		return nil, err
	}

	conn := s.PersistentConfig.GetConnection(req.Read.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrConnectionNotFound.Error())
	}

	// Try to create a backend from given connection options
	be, err := backends.New(conn)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create backend: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Reader needs a unique ID that frontend can reference
	req.Read.XId = uuid.NewV4().String()

	var md *desc.MessageDescriptor

	if err := s.populateDecodeSchemaDetails(req.Read); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	// TODO: can we move this elsewhere?
	if req.Read.DecodeOptions != nil && req.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		var mdErr error

		pbSettings := req.Read.DecodeOptions.ProtobufSettings

		md, mdErr = pb.GetMDFromDescriptorBlob(pbSettings.XMessageDescriptor, pbSettings.ProtobufRootMessage)
		if mdErr != nil {
			return nil, CustomError(common.Code_FAILED_PRECONDITION, fmt.Sprintf("unable to get message descriptor: %s", err))
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// Launch read and record
	read := &types.Read{
		AttachedClients:      make(map[string]*types.AttachedStream, 0),
		AttachedClientsMutex: &sync.RWMutex{},
		PlumberID:            s.PersistentConfig.PlumberID,
		ReadOptions:          req.Read,
		ContextCxl:           ctx,
		CancelFunc:           cancelFunc,
		Backend:              be,
		MsgDesc:              md,
		Log:                  s.Log.WithField("read_id", req.Read.XId),
	}

	s.PersistentConfig.SetRead(req.Read.XId, read)

	go s.beginRead(ctx, read)

	return &protos.CreateReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Read started",
			RequestId: requestID,
		},
		ReadId: req.Read.XId,
	}, nil
}

func (s *Server) populateCachedSchema() {
	//
}

// TODO: Need to figure out how reads work with clustered plumber - do all nodes
// perform a read? If so, should StopRead() inform other nodes to stop reading
// as well?
func (s *Server) StopRead(_ context.Context, req *protos.StopReadRequest) (*protos.StopReadResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := s.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	if !read.ReadOptions.XActive {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "Read is already stopped")
	}

	read.CancelFunc()

	read.ReadOptions.XActive = false

	s.Log.WithField("request_id", requestID).Infof("Read '%s' stopped", req.ReadId)

	return &protos.StopReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Read stopped",
			RequestId: requestID,
		},
	}, nil
}

func (s *Server) ResumeRead(_ context.Context, req *protos.ResumeReadRequest) (*protos.ResumeReadResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := s.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	if read.ReadOptions.XActive {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "Read is already active")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	conn := s.PersistentConfig.GetConnection(read.ReadOptions.ConnectionId)
	if conn == nil {
		cancelFunc()
		return nil, CustomError(common.Code_ABORTED, validate.ErrConnectionNotFound.Error())
	}

	// Try to create a backend from given connection options
	be, err := backends.New(conn)
	if err != nil {
		cancelFunc()
		return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create backend: %s", err))
	}

	// Fresh connection and context
	read.Backend = be
	read.ContextCxl = ctx
	read.CancelFunc = cancelFunc
	read.ReadOptions.XActive = true

	go s.beginRead(ctx, read)

	s.Log.WithField("request_id", requestID).Infof("Read '%s' resumed", req.ReadId)

	return &protos.ResumeReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Message read",
			RequestId: requestID,
		},
	}, nil
}

func (s *Server) DeleteRead(_ context.Context, req *protos.DeleteReadRequest) (*protos.DeleteReadResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := s.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	// Stop it if it's in progress
	if read.ReadOptions.XActive {
		read.CancelFunc()
	}

	s.PersistentConfig.DeleteRead(req.ReadId)

	s.Log.WithField("request_id", requestID).Infof("Read '%s' deleted", req.ReadId)

	return &protos.DeleteReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Read Deleted",
			RequestId: requestID,
		},
	}, nil
}

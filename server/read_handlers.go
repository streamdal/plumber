package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/batchcorp/plumber/server/types"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (p *Server) GetAllReads(_ context.Context, req *protos.GetAllReadsRequest) (*protos.GetAllReadsResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	reads := make([]*protos.Read, 0)

	for _, v := range p.PersistentConfig.Reads {
		reads = append(reads, v.ReadOptions)
	}

	return &protos.GetAllReadsResponse{
		Read: reads,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *Server) StartRead(req *protos.StartReadRequest, srv protos.PlumberServer_StartReadServer) error {
	requestID := uuid.NewV4().String()

	if err := p.validateRequest(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if req.ReadId == "" {
		return CustomError(common.Code_FAILED_PRECONDITION, "read not found")
	}

	read := p.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return CustomError(common.Code_NOT_FOUND, "read not found")
	}

	stream := &types.AttachedStream{
		MessageCh: make(chan *records.Message, 1000),
	}

	read.AttachedClientsMutex.Lock()
	read.AttachedClients[requestID] = stream
	read.AttachedClientsMutex.Unlock()

	llog := p.Log.WithField("read_id", read.ReadOptions.Id).
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
			messages := make([]*records.Message, 0)

			// TODO: batch these up and send multiple per response?
			messages = append(messages, msg)

			res := &protos.StartReadResponse{
				Status: &common.Status{
					Code:      common.Code_OK,
					Message:   "Message read",
					RequestId: requestID,
				},
				Messages: messages,
			}
			if err := srv.Send(res); err != nil {
				llog.Error(err)
				continue
			}
			llog.Debugf("Sent message to client '%s'", requestID)
		case <-read.ContextCxl.Done():
			// StartRead stopped. close out all streams for it
			llog.Debugf("StartRead stopped. closing stream for client '%s'", requestID)
			return nil
		default:
			// NOOP
		}
	}
}

func (p *Server) CreateRead(_ context.Context, req *protos.CreateReadRequest) (*protos.CreateReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	readCfg := req.GetRead()
	if err := validateRead(readCfg); err != nil {
		return nil, err
	}

	requestID := uuid.NewV4().String()

	// Reader needs a unique ID that frontend can reference
	readCfg.Id = uuid.NewV4().String()

	md, err := p.getMessageDescriptor(readCfg.GetDecodeOptions())
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	backend, err := p.getBackendRead(readCfg)
	if err != nil {
		cancelFunc()
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Launch read and record
	read := &types.Read{
		AttachedClients:      make(map[string]*types.AttachedStream, 0),
		AttachedClientsMutex: &sync.RWMutex{},
		PlumberID:            p.PersistentConfig.PlumberID,
		ReadOptions:          readCfg,
		ContextCxl:           ctx,
		CancelFunc:           cancelFunc,
		Backend:              backend,
		MsgDesc:              md,
		Log:                  p.Log.WithField("read_id", readCfg.Id),
	}

	p.PersistentConfig.SetRead(readCfg.Id, read)

	var offsetStart, offsetStep int64

	// Can't wait forever. If no traffic on the topic after 2 minutes, cancel sample call
	timeoutCtx, _ := context.WithTimeout(context.Background(), types.SampleOffsetInterval*2)

	var sampleRate time.Duration

	if readCfg.GetSampleOptions() != nil {
		offsetStep, offsetStart, err = read.GetSampleRate(timeoutCtx)
		if errors.Is(err, context.DeadlineExceeded) {
			p.Log.Warnf("Could not get sample rate: %s", err.Error())
			return nil, CustomError(common.Code_ABORTED, "could not calculate sample rate: "+err.Error())
		}

		p.Log.Debugf("Offset step rate: %d, starting at offset %d", offsetStep, offsetStart)

		if offsetStart == types.NoSample {
			p.Log.Warn("No traffic on message bus, falling back to time based rate")
			sr, err := getFallBackSampleRate(readCfg.SampleOptions)
			if err != nil {
				return nil, CustomError(common.Code_ABORTED, "could not calculate sample rate: "+err.Error())
			}
			p.Log.Debugf("Fallback sample rate is 1 message every %dms", sr.Milliseconds())
			sampleRate = sr
		}
	}

	read.SampleStart = offsetStart
	read.SampleStep = offsetStep
	read.FallbackSampleRate = sampleRate

	go read.StartRead()

	return &protos.CreateReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "StartRead started",
			RequestId: requestID,
		},
		ReadId: readCfg.Id,
	}, nil
}

// getFallBackSampleRate returns a ticker to use to try and read messages from the bus at a given interval.
// This is used when there is no message bus traffic, so we can't infer an offset rate
func getFallBackSampleRate(sampleOpts *protos.SampleOptions) (time.Duration, error) {
	switch sampleOpts.SampleInterval {
	case protos.SampleOptions_MINUTE:
		ms := time.Minute.Milliseconds() / int64(sampleOpts.SampleRate)
		d, err := time.ParseDuration(fmt.Sprintf("%dms", ms))
		if err != nil {
			return 0, err
		}
		return d, nil
	case protos.SampleOptions_SECOND:
		ms := time.Second.Milliseconds() / int64(sampleOpts.SampleRate)
		d, err := time.ParseDuration(fmt.Sprintf("%dms", ms))
		if err != nil {
			return 0, err
		}
		return d, nil
	default:
		return 0, fmt.Errorf("unknown sample interval: '%d'", sampleOpts.SampleInterval)
	}
}

func (p *Server) StopRead(_ context.Context, req *protos.StopReadRequest) (*protos.StopReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := p.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	if !read.ReadOptions.Active {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "Read is already stopped")
	}

	read.CancelFunc()

	read.ReadOptions.Active = false

	p.Log.WithField("request_id", requestID).Infof("Read '%s' stopped", req.ReadId)

	return &protos.StopReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Message read",
			RequestId: requestID,
		},
	}, nil
}

func (p *Server) ResumeRead(_ context.Context, req *protos.ResumeReadRequest) (*protos.ResumeReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := p.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	if read.ReadOptions.Active {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "Read is already active")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	backend, err := p.getBackendRead(read.ReadOptions)
	if err != nil {
		cancelFunc()
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Fresh connection and context
	read.Backend = backend
	read.ContextCxl = ctx
	read.CancelFunc = cancelFunc
	read.ReadOptions.Active = true

	go read.StartRead()

	p.Log.WithField("request_id", requestID).Infof("Read '%s' resumed", req.ReadId)

	return &protos.ResumeReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Message read",
			RequestId: requestID,
		},
	}, nil
}

func (p *Server) DeleteRead(_ context.Context, req *protos.DeleteReadRequest) (*protos.DeleteReadResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	// Get reader and cancel
	read := p.PersistentConfig.GetRead(req.ReadId)
	if read == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "read does not exist or has already been stopped")
	}

	// Stop it if it's in progress
	if read.ReadOptions.Active {
		read.CancelFunc()
	}

	p.PersistentConfig.DeleteRead(req.ReadId)

	p.Log.WithField("request_id", requestID).Infof("Read '%s' deleted", req.ReadId)

	return &protos.DeleteReadResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Read Deleted",
			RequestId: requestID,
		},
	}, nil
}

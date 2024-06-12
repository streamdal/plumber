package streamdal

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"golang.org/x/time/rate"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"

	"github.com/streamdal/streamdal/sdks/go/logger"
	"github.com/streamdal/streamdal/sdks/go/metrics"
	"github.com/streamdal/streamdal/sdks/go/server"
	"github.com/streamdal/streamdal/sdks/go/types"
	"github.com/streamdal/streamdal/sdks/go/validate"
)

const (
	// NumTailWorkers is the number of tail workers to start for each tail request
	// The workers are responsible for reading from the tail channel and streaming
	// TailResponse messages to the server
	NumTailWorkers = 2

	// MinTailResponseIntervalMS is how often we send a TailResponse to the server
	// If this rate is exceeded, we will drop messages rather than flooding the server
	// This is an int to avoid a .Milliseconds() call
	MinTailResponseIntervalMS = 10
)

type Tail struct {
	Request    *protos.Command
	CancelFunc context.CancelFunc

	outboundCh      chan *protos.TailResponse
	streamdalServer server.IServerClient
	metrics         metrics.IMetrics
	cancelCtx       context.Context
	lastMsg         time.Time
	log             logger.Logger
	active          bool
	limiter         *rate.Limiter
}

func (t *Tail) ShouldSend() bool {
	// If no rate limit, allow all messages
	if t.limiter == nil {
		return true
	}

	return t.limiter.Allow()
}

func (s *Streamdal) sendTail(aud *protos.Audience, pipelineID string, originalData []byte, postPipelineData []byte) {
	tails := s.getTailsForAudience(aud)
	if len(tails) == 0 {
		return
	}

	for _, tail := range tails {
		tailID := tail.Request.GetTail().Request.Id

		if !tail.active {
			tail.log.Debugf("tail id '%s' is not active - starting workers", tailID)

			if err := tail.startWorkers(); err != nil {
				tail.log.Errorf("error starting tail workers for request '%s': %s", err, tailID)
				continue
			}

			// Save tail state
			tail.active = true
		}

		if !tail.ShouldSend() {
			continue
		}

		tr := &protos.TailResponse{
			Type:          protos.TailResponseType_TAIL_RESPONSE_TYPE_PAYLOAD,
			TailRequestId: tailID,
			Audience:      aud,
			PipelineId:    pipelineID,
			SessionId:     s.sessionID,
			TimestampNs:   time.Now().UTC().UnixNano(),
			OriginalData:  originalData,
			NewData:       postPipelineData,
		}

		tail.ShipResponse(tr)
	}
}

func (t *Tail) ShipResponse(tr *protos.TailResponse) {
	// If we're sending too fast, drop the message
	if time.Since(t.lastMsg).Milliseconds() < MinTailResponseIntervalMS {
		_ = t.metrics.Incr(context.Background(), &types.CounterEntry{
			Name:   types.DroppedTailMessages,
			Labels: map[string]string{},
			Value:  1})

		t.log.Warnf("sending tail responses too fast - dropping message for tail request id '%s'", tr.TailRequestId)
		return
	}

	t.outboundCh <- tr
	t.lastMsg = time.Now()
}

func (t *Tail) startWorkers() error {
	for i := 0; i < NumTailWorkers; i++ {
		// Start SDK -> Server streaming gRPC connection
		stream, err := t.streamdalServer.GetTailStream(t.cancelCtx)
		if err != nil {
			return errors.Wrap(err, "error starting tail worker")
		}

		looper := director.NewFreeLooper(director.FOREVER, make(chan error, 1))

		go t.startWorker(looper, stream)
	}

	return nil
}

func (t *Tail) startWorker(looper director.Looper, stream protos.Internal_SendTailClient) {
	if stream == nil {
		t.log.Error("stream is nil, unable to start tail worker")
		return
	}

	// Always cancel the context regardless of how we exit so
	// that getTailsForAudience() can remove the tail from the map
	defer t.CancelFunc()

	var quit bool

	looper.Loop(func() error {
		if quit {
			time.Sleep(time.Millisecond * 50)
			return nil
		}

		select {
		case <-t.cancelCtx.Done():
			t.log.Debug("tail worker cancelled")
			quit = true
			looper.Quit()
			return nil
		case <-stream.Context().Done():
			t.log.Debug("tail worker context terminated")
			quit = true
			looper.Quit()
			return nil
		case resp := <-t.outboundCh:
			if err := stream.Send(resp); err != nil {
				if strings.Contains(err.Error(), io.EOF.Error()) {
					t.log.Debug("tail worker received EOF, exiting")
					return nil
				}
				if strings.Contains(err.Error(), "connection refused") {
					// Streamdal server went away, log, sleep, and wait for reconnect
					t.log.Warn("failed to send tail response, streamdal server went away, waiting for reconnect")
					time.Sleep(ReconnectSleep)
					return nil
				}
				t.log.Errorf("error sending tail: %s", err)
			}
		}
		return nil
	})
}

// startTailHandler will record the start tail request in the main tail map.
// Starting of the tail workers is done in the sendTail() method which is called
// during .Process() calls.
func (s *Streamdal) startTailHandler(_ context.Context, cmd *protos.Command) error {
	if err := validate.TailRequestStartCommand(cmd); err != nil {
		return errors.Wrap(err, "invalid tail command")
	}

	// Check if we have this audience
	audStr := audToStr(cmd.Audience)

	// If this is a start that was given to us as part of an initial Register(),
	// then we will probably not know about the audience(s) yet, so we should
	// keep ALL StartTail requests in memory until we see the audience mentioned
	// in a .Process().
	//
	// Optimization for the future: TTL the tail request for 24h and refresh the
	// the TTL when we see the audience in a .Process() call. This would prevent
	// the tail map from growing indefinitely. ~DS

	s.config.Logger.Debugf("received tail command for audience '%s'; saving to tail map", audStr)

	ctx, cancel := context.WithCancel(s.config.ShutdownCtx)

	// Create entry in tail map
	t := &Tail{
		Request:         cmd,
		outboundCh:      make(chan *protos.TailResponse, 100),
		cancelCtx:       ctx,
		CancelFunc:      cancel,
		streamdalServer: s.serverClient,
		metrics:         s.metrics,
		log:             s.config.Logger,
		lastMsg:         time.Now(),
		active:          false,
	}

	// Convert sample interval from seconds to time.Duration
	if sampleOpts := cmd.GetTail().Request.GetSampleOptions(); sampleOpts != nil {
		// Convert seconds to nanoseconds
		interval := time.Duration(sampleOpts.SampleIntervalSeconds/sampleOpts.SampleRate) * time.Second
		t.limiter = rate.NewLimiter(rate.Every(interval), int(sampleOpts.SampleRate))
	}

	// Save entry in tail map
	s.setActiveTail(t)

	return nil
}

func (s *Streamdal) stopTailHandler(_ context.Context, cmd *protos.Command) error {
	if err := validate.TailRequestStopCommand(cmd); err != nil {
		return errors.Wrap(err, "invalid tail request stop command")
	}

	aud := cmd.GetTail().Request.Audience
	tailID := cmd.GetTail().Request.Id

	tails := s.getTailsForAudience(aud)
	if len(tails) == 0 {
		s.config.Logger.Debugf("Received stop tail command for unknown tail: %s", tailID)
		return nil
	}

	if activeTail, ok := tails[tailID]; ok {
		// Cancel workers
		activeTail.CancelFunc()
	}

	pausedTails := s.getPausedTailsForAudience(aud)
	if pausedTail, ok := pausedTails[tailID]; ok {
		// Cancel workers
		pausedTail.CancelFunc()
	}

	s.removeActiveTail(aud, tailID)
	s.removePausedTail(aud, tailID)

	return nil
}

// k: audience_as_str
func (s *Streamdal) getTailsForAudience(aud *protos.Audience) map[string]*Tail {
	s.tailsMtx.RLock()
	tails, ok := s.tails[audToStr(aud)]
	s.tailsMtx.RUnlock()

	if ok {
		return tails
	}

	return nil
}

func (s *Streamdal) getPausedTailsForAudience(aud *protos.Audience) map[string]*Tail {
	s.pausedTailsMtx.RLock()
	tails, ok := s.pausedTails[audToStr(aud)]
	s.pausedTailsMtx.RUnlock()

	if ok {
		return tails
	}

	return nil
}

func (s *Streamdal) removeActiveTail(aud *protos.Audience, tailID string) {
	s.tailsMtx.Lock()
	defer s.tailsMtx.Unlock()

	audStr := audToStr(aud)

	if _, ok := s.tails[audStr]; !ok {
		return
	}

	delete(s.tails[audStr], tailID)

	if len(s.tails[audStr]) == 0 {
		delete(s.tails, audStr)
	}
}

func (s *Streamdal) removePausedTail(aud *protos.Audience, tailID string) {
	s.pausedTailsMtx.Lock()
	defer s.pausedTailsMtx.Unlock()

	audStr := audToStr(aud)

	if _, ok := s.pausedTails[audStr]; !ok {
		return
	}

	delete(s.pausedTails[audStr], tailID)

	if len(s.pausedTails[audStr]) == 0 {
		delete(s.pausedTails, audStr)
	}
}

func (s *Streamdal) setActiveTail(tail *Tail) {
	s.tailsMtx.Lock()
	defer s.tailsMtx.Unlock()

	tr := tail.Request.GetTail().Request

	audStr := audToStr(tr.Audience)

	if _, ok := s.tails[audStr]; !ok {
		s.tails[audStr] = make(map[string]*Tail)
	}

	s.tails[audStr][tr.Id] = tail
}

func (s *Streamdal) setPausedTail(tail *Tail) {
	s.pausedTailsMtx.Lock()
	defer s.pausedTailsMtx.Unlock()

	tr := tail.Request.GetTail().Request

	audStr := audToStr(tr.Audience)

	if _, ok := s.pausedTails[audStr]; !ok {
		s.pausedTails[audStr] = make(map[string]*Tail)
	}

	s.pausedTails[audStr][tr.Id] = tail
}

func (s *Streamdal) pauseTailHandler(_ context.Context, cmd *protos.Command) error {
	if err := validate.TailRequestPauseCommand(cmd); err != nil {
		return errors.Wrap(err, "invalid tail request pause command")
	}

	aud := cmd.GetTail().Request.Audience
	tailID := cmd.GetTail().Request.Id

	tails := s.getTailsForAudience(aud)
	if len(tails) == 0 {
		s.config.Logger.Debugf("Received pause tail command for unknown tail: %s", tailID)
		return nil
	}

	tail, ok := tails[tailID]
	if !ok {
		s.config.Logger.Debugf("Received pause tail command for unknown tail: %s", tailID)
		return nil
	}

	// Remove from active tails
	s.removeActiveTail(aud, tailID)

	// Mode to paused tails
	s.setPausedTail(tail)

	s.config.Logger.Infof("Paused tail: %s", tailID)

	return nil
}

func (s *Streamdal) resumeTailHandler(_ context.Context, cmd *protos.Command) error {
	if err := validate.TailRequestResumeCommand(cmd); err != nil {
		return errors.Wrap(err, "invalid tail request resume command")
	}

	aud := cmd.GetTail().Request.Audience
	tailID := cmd.GetTail().Request.Id

	tails := s.getPausedTailsForAudience(aud)
	if len(tails) == 0 {
		s.config.Logger.Debugf("Received resume tail command for unknown tail: %s", tailID)
		return nil
	}

	tail, ok := tails[tailID]
	if !ok {
		s.config.Logger.Debugf("Received resume tail command for unknown tail: %s", tailID)
		return nil
	}

	// Remove from paused tails
	s.removePausedTail(aud, tailID)

	// Add to active tails
	s.setActiveTail(tail)

	s.config.Logger.Infof("Resumed tail: %s", tailID)

	return nil
}

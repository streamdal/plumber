package streamdal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"
)

func (s *Streamdal) addAudience(ctx context.Context, aud *protos.Audience) {
	// Don't need to add twice
	if s.seenAudience(ctx, aud) {
		return
	}

	s.audiencesMtx.Lock()

	if s.audiences == nil {
		s.audiences = make(map[string]struct{})
	}

	s.audiences[audToStr(aud)] = struct{}{}
	s.audiencesMtx.Unlock()

	// Run as goroutine to avoid blocking processing
	go func() {
		if err := s.serverClient.NewAudience(ctx, aud, s.sessionID); err != nil {
			s.config.Logger.Errorf("failed to add audience: %s", err)
		}
	}()
}

// addAudiences is used for RE-adding audiences that may have timed out after
// a server reconnect. The method will re-add all known audiences to the server
// via internal gRPC NewAudience() endpoint. This is a non-blocking method.
func (s *Streamdal) addAudiences(ctx context.Context) {
	s.audiencesMtx.RLock()
	defer s.audiencesMtx.RUnlock()

	for audStr := range s.audiences {
		aud := strToAud(audStr)

		if aud == nil {
			s.config.Logger.Errorf("unexpected strToAud resulted in nil audience (audStr: %s)", audStr)
			continue
		}

		// Run as goroutine to avoid blocking processing
		go func() {
			if err := s.serverClient.NewAudience(ctx, aud, s.sessionID); err != nil {
				s.config.Logger.Errorf("failed to add audience: %s", err)
			}
		}()
	}
}

func (s *Streamdal) seenAudience(_ context.Context, aud *protos.Audience) bool {
	s.audiencesMtx.RLock()
	defer s.audiencesMtx.RUnlock()

	if s.audiences == nil {
		return false
	}

	_, ok := s.audiences[audToStr(aud)]
	return ok
}

func (s *Streamdal) getCurrentAudiences() []*protos.Audience {
	s.audiencesMtx.RLock()
	defer s.audiencesMtx.RUnlock()

	auds := make([]*protos.Audience, 0)
	for aud := range s.audiences {
		auds = append(auds, strToAud(aud))
	}

	return auds
}

func audToStr(aud *protos.Audience) string {
	if aud == nil {
		return ""
	}

	return strings.ToLower(fmt.Sprintf("%s:%s:%d:%s", aud.ServiceName, aud.ComponentName, aud.OperationType, aud.OperationName))
}

func strToAud(str string) *protos.Audience {
	if str == "" {
		return nil
	}

	str = strings.ToLower(str)

	parts := strings.Split(str, ":")
	if len(parts) != 4 {
		return nil
	}

	opType, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil
	}

	return &protos.Audience{
		ServiceName:   parts[0],
		ComponentName: parts[1],
		OperationType: protos.OperationType(opType),
		OperationName: parts[3],
	}
}

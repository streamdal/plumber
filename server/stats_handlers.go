package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (s *Server) GetCurrentCounters(ctx context.Context, req *protos.GetCurrentCountersRequest) (*protos.GetCurrentCountersResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	counters := make([]*opts.Counter, 0)

	allCounters := s.StatsService.GetAllCounters()
	for _, c := range allCounters {
		cfg := c.GetConfig()

		total, err := c.GetTotal()
		if err != nil {
			s.Log.Errorf("unable to get total for counter '%s': %s", cfg.ResourceId, err)
			continue
		}

		counters = append(counters, &opts.Counter{
			Resource:   cfg.Resource,
			Type:       cfg.Type,
			ResourceId: cfg.ResourceId,
			Value:      total,
		})
	}

	return &protos.GetCurrentCountersResponse{
		Counters: counters,
	}, nil
}
func (s *Server) GetCounterHistory(ctx context.Context, req *protos.GetCounterHistoryRequest) (*protos.GetCounterHistoryResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	c, err := s.StatsService.GetCounter(req.Type, req.Resource, req.ResourceId)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	history := make([]*protos.CounterDataPoint, 0)

	dps, err := c.GetTSHistory(req.FromUnixTs, req.ToUnixTs)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	for _, dp := range dps {
		history = append(history, &protos.CounterDataPoint{
			UnixTs: dp.Timestamp,
			Value:  dp.Value,
		})
	}

	return &protos.GetCounterHistoryResponse{
		Counter:    c.GetConfig(),
		DataPoints: history,
	}, nil
}

package server

import (
	"context"
	"errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetRules(_ context.Context, in *protos.GetDataQualityRulesRequest) (*protos.GetDataQualityRulesResponse, error) {
	return nil, errors.New("not implemented")
}

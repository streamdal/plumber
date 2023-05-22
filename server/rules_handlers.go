package server

import (
	"context"
	"errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetRules(_ context.Context, req *protos.GetDataQualityRulesRequest) (*protos.GetDataQualityRulesResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *Server) SendRuleNotification(_ context.Context, req *protos.SendRuleNotificationRequest) (*protos.SendRuleNotificationResponse, error) {
	return nil, errors.New("not implemented")

	// TODO: get rule based on ID

	// TODO: determine which alert we need to send

	// TODO: how to handle slack integration? I think just accept bot token initially and then allow
	// TODO: some kind of oauth integration
}

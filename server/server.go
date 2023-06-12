package server

import (
	"context"
	"fmt"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/actions"
	"github.com/batchcorp/plumber/bus"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/kv"
	"github.com/batchcorp/plumber/validate"
)

type Server struct {
	Actions          actions.IActions
	AuthToken        string
	PersistentConfig *config.Config
	Bus              bus.IBus
	Log              *logrus.Entry
	CLIOptions       *opts.CLIOptions
	KV               kv.IKV
	DataAlerts       chan *protos.SendRuleNotificationRequest
	ShutdownCtx      context.Context
}

func (s *Server) GetServerOptions(_ context.Context, req *protos.GetServerOptionsRequest) (*protos.GetServerOptionsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return &protos.GetServerOptionsResponse{
		ServerOptions: &opts.ServerOptions{
			NodeId:            s.CLIOptions.Server.NodeId,
			ClusterId:         s.CLIOptions.Server.ClusterId,
			GrpcListenAddress: s.CLIOptions.Server.GrpcListenAddress,
			AuthToken:         s.CLIOptions.Server.AuthToken,
		},
	}, nil
}

type ErrorWrapper struct {
	Status *common.Status
}

func (e *ErrorWrapper) Error() string {
	return e.Status.Message
}

func CustomError(c common.Code, msg string) error {
	return &ErrorWrapper{
		Status: &common.Status{
			Code:      c,
			Message:   msg,
			RequestId: uuid.NewV4().String(),
		},
	}
}

func (s *Server) validateAuth(auth *common.Auth) error {
	if auth == nil {
		return validate.ErrMissingAuth
	}

	if auth.Token != s.AuthToken {
		return validate.ErrInvalidToken
	}

	return nil
}

func (s *Server) StartRuleAlerts() {
	s.Log.Debug("starting rule alerts notifier")

	for {
		select {
		case <-s.ShutdownCtx.Done():
			s.Log.Debug("shutting down rule alerts notifier")
			return
		case req := <-s.DataAlerts:

			// Get rule set
			ruleSet := s.PersistentConfig.GetRuleSet(req.RulesetId)
			if ruleSet == nil {
				s.Log.Errorf("rule set '%s' not found", req.RulesetId)
				break
			}

			// Get rule from rule set
			rule, ok := ruleSet.Set.Rules[req.RuleId]
			if !ok {
				s.Log.Errorf("rule '%s' not found in rule set '%s'", req.RuleId, ruleSet.Set.Name)
				break
			}

			s.handleRuleNotificationRequest(ruleSet.Set, rule, req)
		}
	}
}

func (s *Server) handleRuleNotificationRequest(ruleSet *common.RuleSet, rule *common.Rule, req *protos.SendRuleNotificationRequest) {
	for _, cfg := range rule.FailureModeConfigs {
		switch cfg.Mode {
		case common.RuleFailureMode_RULE_FAILURE_MODE_DLQ:
			if err := s.sendRuleToDLQ(req.Data, ruleSet.Name, rule, cfg.GetDlq()); err != nil {
				s.Log.Error(err)
			}
			s.Log.Errorf("Sent message to DLQ for rule '%s' in rule set '%s'", rule.Id, ruleSet.Name)
		case common.RuleFailureMode_RULE_FAILURE_MODE_ALERT_SLACK:
			if err := s.sendRuleSlackNotification(req.Data, ruleSet.Name, rule, cfg.GetAlertSlack()); err != nil {
				s.Log.Error(err)
			}
			s.Log.Errorf("Sent slack notification for rule '%s' in rule set '%s'", rule.Id, ruleSet.Name)
		default:
			s.Log.Errorf("unknown failure mode '%s' for rule '%s' in rule set '%s'", cfg.Mode.String(), rule.Id, ruleSet.Name)
		}
	}
}

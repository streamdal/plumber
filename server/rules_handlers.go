package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/slack-go/slack"
	"google.golang.org/grpc"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/util"
)

const (
	// MaxGRPCRetries is the number of times we will attempt a GRPC call before giving up
	MaxGRPCRetries = 5

	// GRPCRetrySleep determines how long we sleep between GRPC call retries
	GRPCRetrySleep = time.Second * 5

	// MaxGRPCMessageSize is the maximum message size for GRPC client in bytes
	MaxGRPCMessageSize = 1024 * 1024 * 100 // 100MB
)

func (s *Server) GetRuleSets(_ context.Context, req *protos.GetDataQualityRuleSetsRequest) (*protos.GetDataQualityRuleSetsResponse, error) {
	ruleSets := make([]*common.RuleSet, 0)

	s.PersistentConfig.RuleSetMutex.RLock()
	for _, ruleSet := range s.PersistentConfig.RuleSets {
		if ruleSet.Set.DataSource == req.DataSource {
			ruleSets = append(ruleSets, ruleSet.Set)
		}
	}
	s.PersistentConfig.RuleSetMutex.RUnlock()

	return &protos.GetDataQualityRuleSetsResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
		RuleSets: ruleSets,
	}, nil
}

func (s *Server) SendRuleNotification(_ context.Context, req *protos.SendRuleNotificationRequest) (*protos.SendRuleNotificationResponse, error) {
	// Get rule set
	ruleSet := s.PersistentConfig.GetRuleSet(req.RulesetId)
	if ruleSet == nil {
		return nil, errors.New("rule set not found")
	}

	// Get rule from rule set
	_, ok := ruleSet.Set.Rules[req.RuleId]
	if !ok {
		return nil, errors.New("rule not found")
	}

	s.DataAlerts <- req

	return &protos.SendRuleNotificationResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) sendRuleSlackNotification(_ []byte, name string, rule *common.Rule, cfg *common.FailureModeAlertSlack) error {
	if rule == nil {
		return errors.New("BUG: rule is nil")
	}

	if cfg == nil {
		return errors.New("BUG: alert slack config is nil")
	}

	api := slack.New(s.PersistentConfig.SlackToken)

	var blocks []*slack.TextBlockObject

	switch rule.Type {
	case common.RuleType_RULE_TYPE_MATCH:
		match := rule.GetMatchConfig()
		blocks = []*slack.TextBlockObject{
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Rule Set*: \n%s\n", name), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Rule ID*: \n%s\n", rule.Id), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Rule Type*: \n%s\n", "Match"), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Match Type*: \n%s\n", match.Type), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Match Path*: \n%s\n", match.Path), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Match Args*: \n%s\n", strings.Join(match.Args, ",")), false, false),
		}
	case common.RuleType_RULE_TYPE_CUSTOM:
		// TODO: implement further down the line
	}

	headerBlock := slack.NewHeaderBlock(slack.NewTextBlockObject(slack.PlainTextType, "Data Quality Alert", false, false))
	sectionBlock := slack.NewSectionBlock(nil, blocks, nil)

	divBlock := slack.NewDividerBlock()

	_, _, err := api.PostMessage(
		cfg.SlackChannel,
		slack.MsgOptionBlocks(headerBlock, divBlock, sectionBlock, divBlock),
		slack.MsgOptionAsUser(true),
	)
	if err != nil {
		err = errors.Wrapf(err, "unable to send slack alert")
		return err
	}

	return nil
}

// TODO: need some kind of connection pooling and also a channel
func (s *Server) sendRuleToDLQ(data []byte, name string, rule *common.Rule, cfg *common.FailureModeDLQ) error {
	if rule == nil {
		return errors.New("BUG: rule is nil")
	}

	if cfg == nil {
		return errors.New("BUG: dlq config is nil")
	}

	record := &records.GenericRecord{
		ForceDeadLetter: true,
		Body:            data,
		Source:          "data_quality",
		Timestamp:       time.Now().UTC().UnixNano(),
		Metadata: map[string]string{
			"data_quality_rule_set": name,
			"plumber_id":            s.PersistentConfig.PlumberID,
			"plumber_version":       s.PersistentConfig.LastVersion,
			"plumber_cluster_id":    s.PersistentConfig.ClusterID,
			"rule_id":               rule.Id,
		},
	}

	// TODO: we need gRPC connection params in server protos
	// TODO: currently they are only in relays
	const (
		gGRPCAddress = "localhost:9000"
		timeout      = time.Second * 5
		disableTLS   = true
	)

	conn, outboundCtx, err := util.NewGRPCConnection(gGRPCAddress, cfg.StreamdalToken, timeout, disableTLS, true)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC connection")
	}
	defer conn.Close()

	client := services.NewGRPCCollectorClient(conn)

	return s.CallWithRetry(outboundCtx, "AddGenericRecord", func(ctx context.Context) error {
		_, err := client.AddRecord(ctx, &services.GenericRecordRequest{
			Records: []*records.GenericRecord{record},
		}, grpc.MaxCallSendMsgSize(MaxGRPCMessageSize))
		return err
	})
}

func (s *Server) CallWithRetry(ctx context.Context, method string, publish func(ctx context.Context) error) error {
	var err error

	for i := 1; i <= MaxGRPCRetries; i++ {
		err = publish(ctx)
		if err != nil {
			prometheus.IncrPromCounter(prometheus.PlumberGRPCErrors, 1)

			// Paused collection, retries will fail, exit early
			if strings.Contains(err.Error(), "collection is paused") {
				return err
			}
			s.Log.Debugf("unable to complete %s call [retry %d/%d]", method, i, 5)
			time.Sleep(GRPCRetrySleep)
			continue
		}
		s.Log.Debugf("successfully handled %s message", strings.Replace(method, "Add", "", 1))
		return nil
	}

	return fmt.Errorf("unable to complete %s call [reached max retries (%d)]: %s", method, MaxGRPCRetries, err)
}

func (s *Server) PublishMetrics(ctx context.Context, req *protos.PublishMetricsRequest) (*protos.PublishMetricsResponse, error) {
	c := &types.Counter{
		Type:  req.Counter,
		Value: req.Value,
	}

	if err := s.Actions.Counter(ctx, c); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	if err := s.Bus.PublishCounter(ctx, c); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	return &protos.PublishMetricsResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Metrics collected",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) GetRule(ctx context.Context, req *protos.GetDataQualityRuleRequest) (*protos.GetDataQualityRuleResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method GetRule not implemented")
}
func (s *Server) CreateRule(ctx context.Context, req *protos.CreateDataQualityRuleRequest) (*protos.CreateDataQualityRuleResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method CreateRule not implemented")
}
func (s *Server) UpdateRule(ctx context.Context, req *protos.UpdateDataQualityRuleRequest) (*protos.UpdateDataQualityRuleResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method UpdateRule not implemented")
}
func (s *Server) DeleteRule(ctx context.Context, req *protos.DeleteDataQualityRuleRequest) (*protos.DeleteDataQualityRuleResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method DeleteRule not implemented")
}
func (s *Server) CreateRuleSet(ctx context.Context, req *protos.CreateDataQualityRuleSetRequest) (*protos.CreateDataQualityRuleSetResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method CreateRuleSet not implemented")
}
func (s *Server) UpdateRuleSet(ctx context.Context, req *protos.UpdateDataQualityRuleSetRequest) (*protos.UpdateDataQualityRuleSetResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method UpdateRuleSet not implemented")
}
func (s *Server) DeleteRuleSet(ctx context.Context, req *protos.DeleteDataQualityRuleSetRequest) (*protos.DeleteDataQualityRuleSetResponse, error) {
	return nil, CustomError(common.Code_UNIMPLEMENTED, "method DeleteRuleSet not implemented")
}

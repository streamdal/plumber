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

func (s *Server) GetRules(_ context.Context, req *protos.GetDataQualityRulesRequest) (*protos.GetDataQualityRulesResponse, error) {
	ruleSets := make([]*common.RuleSet, 0)

	s.PersistentConfig.RuleSetMutex.RLock()
	for _, ruleSet := range s.PersistentConfig.RuleSets {
		ruleSets = append(ruleSets, ruleSet.Set)
	}
	s.PersistentConfig.RuleSetMutex.RUnlock()

	return &protos.GetDataQualityRulesResponse{
		Status: &common.Status{
			Code: common.Code_OK,
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
	rule, ok := ruleSet.Set.Rules[req.RuleId]
	if !ok {
		return nil, errors.New("rule not found")
	}

	switch rule.FailureMode {
	case common.RuleFailureMode_RULE_FAILURE_MODE_DLQ:
		if err := s.sendRuleSlackNotification(req.Data, ruleSet.Set.Name, rule); err != nil {
			return nil, CustomError(common.Code_UNKNOWN, err.Error())
		}
	case common.RuleFailureMode_RULE_FAILURE_MODE_ALERT_SLACK:
		if err := s.sendRuleToDLQ(req.Data, rule.GetDlq()); err != nil {
			return nil, CustomError(common.Code_UNKNOWN, err.Error())
		}
	default:
		return nil, errors.New("invalid failure mode")
	}

	return &protos.SendRuleNotificationResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) sendRuleSlackNotification(data []byte, name string, rule *common.Rule) error {
	if rule.GetAlertSlack() == nil {
		return errors.New("BUG: alert slack config is nil")
	}

	api := slack.New(s.PersistentConfig.SlackToken)

	var blocks []*slack.TextBlockObject

	switch rule.Type {
	case common.RuleType_RULE_TYPE_TRANSFORM:
		transform := rule.GetTransform()
		blocks = []*slack.TextBlockObject{
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Rule Set*: \n%s\n", name), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, "*Rule Type*: Transform\n", false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*JSON Path*: %s\n", transform.Path), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Value*: %s\n", transform.Value), false, false),
		}
	case common.RuleType_RULE_TYPE_MATCH:
		match := rule.GetMatchConfig()
		blocks = []*slack.TextBlockObject{
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Rule Set*: \n%s\n", name), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Rule Type*: \n%s\n", "Match"), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Match Type*: \n%s\n", match.Type), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Match Path*: \n%s\n", match.Path), false, false),
			slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*Match Args*: \n%s\n", match.Args), false, false),
		}
	}

	headerBlock := slack.NewHeaderBlock(slack.NewTextBlockObject(slack.PlainTextType, "Data Quality Alert", false, false))
	sectionBlock := slack.NewSectionBlock(nil, blocks, nil)

	divBlock := slack.NewDividerBlock()

	_, _, err := api.PostMessage(
		rule.GetAlertSlack().SlackChannel,
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
func (s *Server) sendRuleToDLQ(data []byte, cfg *common.FailureModeDLQ) error {
	record := &records.GenericRecord{
		Body:            data,
		Source:          "plumber",
		Timestamp:       time.Now().UTC().UnixNano(),
		Metadata:        make(map[string]string),
		ForceDeadLetter: true,
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
			prometheus.IncrPromCounter("plumber_grpc_errors", 1)

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

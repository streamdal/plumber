package api

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"

	"github.com/golang/protobuf/jsonpb"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	counters "github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
)

type SlackConfigRequest struct {
	Token string `json:"token"`
}

func (a *API) getRuleSetsHandler(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	a.PersistentConfig.RuleSetMutex.RLock()
	defer a.PersistentConfig.RuleSetMutex.RUnlock()

	WriteJSON(http.StatusOK, a.PersistentConfig.RuleSets, w)
}

func (a *API) createRuleSetHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := r.Context()

	if err := a.PersistentConfig.BootstrapWASMFiles(r.Context()); err != nil {
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
		return
	}

	rs := &common.RuleSet{}

	if err := DecodeProtoBody(r.Body, rs); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	id := uuid.NewV4().String()

	rs.Id = id
	rs.Version = 1

	// Force rule ID setting
	for k, v := range rs.Rules {
		v.Id = k
	}

	if err := a.Bus.PublishCreateRuleSet(ctx, rs); err != nil {
		err = errors.Wrap(err, "unable to publish create rule set event")
		a.log.Error(err)
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
		return
	}

	a.log.Debug("Published create rule set event")

	a.PersistentConfig.SetRuleSet(id, &types.RuleSet{Set: rs})
	_ = a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule set created", Values: map[string]string{"id": id}}, w)
}

func (a *API) updateRuleSetHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := r.Context()

	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	update := &common.RuleSet{}

	if err := DecodeProtoBody(r.Body, update); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	rs.Set.Rules = update.Rules
	rs.Set.Name = update.Name
	rs.Set.Mode = update.Mode
	rs.Set.Key = update.Key
	rs.Set.DataSource = update.DataSource
	rs.Set.Version++

	if err := a.Bus.PublishUpdateRuleSet(ctx, rs.Set); err != nil {
		err = errors.Wrap(err, "unable to publish update rule set event")
		a.log.Error(err)
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
		return
	}

	a.log.Debug("Published create rule set event")

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	_ = a.PersistentConfig.Save()

	a.Bus.PublishUpdateRuleSet(ctx, rs.Set)

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule set updated"}, w)
}

func (a *API) deleteRuleSetHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := r.Context()

	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	if err := a.Bus.PublishDeleteRuleSet(ctx, &common.RuleSet{Id: rs.Set.Id}); err != nil {
		err = errors.Wrap(err, "unable to publish delete  rule set event")
		a.log.Error(err)
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
	}

	a.PersistentConfig.DeleteRuleSet(rs.Set.Id)
	_ = a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule set deleted"}, w)
}

func (a *API) getRuleSetHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	set := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if set == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	WriteJSON(http.StatusOK, set, w)
}

func (a *API) getRulesHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	data, err := marshalRules(rs.Set.Rules)
	if err != nil {
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
	}

	WriteJSON(http.StatusOK, data, w)
}

func (a *API) createRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := r.Context()

	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	rule := &common.Rule{}

	if err := DecodeProtoBody(r.Body, rule); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	id := uuid.NewV4().String()
	rule.Id = id

	if rs.Set.Rules == nil {
		rs.Set.Rules = make(map[string]*common.Rule)
	}
	rs.Set.Rules[id] = rule

	if err := a.Bus.PublishUpdateRuleSet(ctx, rs.Set); err != nil {
		err = errors.Wrap(err, "unable to publish update rule set event")
		a.log.Error(err)
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
		return
	}

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule created", Values: map[string]string{"id": id}}, w)
}

func (a *API) updateRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := r.Context()

	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	rule := rs.Set.Rules[p.ByName("id")]
	if rule == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule not found"}, w)
		return
	}

	update := &common.Rule{}
	if err := DecodeProtoBody(r.Body, update); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	update.Id = rule.Id
	rs.Set.Rules[rule.Id] = update
	rs.Set.Version++

	if err := a.Bus.PublishUpdateRuleSet(ctx, rs.Set); err != nil {
		err = errors.Wrap(err, "unable to publish update rule set event")
		a.log.Error(err)
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
		return
	}

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule updated"}, w)
}

func (a *API) deleteRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	ctx := r.Context()

	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	delete(rs.Set.Rules, p.ByName("id"))
	rs.Set.Version++

	if err := a.Bus.PublishUpdateRuleSet(ctx, rs.Set); err != nil {
		err = errors.Wrap(err, "unable to publish update rule set event")
		a.log.Error(err)
		WriteErrorJSON(http.StatusInternalServerError, err.Error(), w)
		return
	}

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule deleted"}, w)
}

func (a *API) tempPopulateHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	a.PersistentConfig.RuleSetMutex.Lock()
	defer a.PersistentConfig.RuleSetMutex.Unlock()

	a.PersistentConfig.RuleSets = make(map[string]*types.RuleSet)

	id1 := uuid.NewV4().String()
	ruleid1 := uuid.NewV4().String()

	// Fake publish stats
	counters.GetVecCounter(counters.SnitchSubsystem, "publish").
		With(prometheus.Labels{"type": "count", "data_source": "kafka"}).
		Add(float64(rand.Int63n(10000)))
	counters.GetVecCounter(counters.SnitchSubsystem, "publish").
		With(prometheus.Labels{"type": "bytes", "data_source": "kafka"}).
		Add(float64(rand.Int63n(100000)))

	// Fake consume stats
	counters.GetVecCounter(counters.SnitchSubsystem, "consume").
		With(prometheus.Labels{"type": "count", "data_source": "kafka"}).
		Add(float64(rand.Int63n(10000)))
	counters.GetVecCounter(counters.SnitchSubsystem, "consume").
		With(prometheus.Labels{"type": "bytes", "data_source": "kafka"}).
		Add(float64(rand.Int63n(100000)))

	// Fake size exceeded stats
	counters.GetVecCounter(counters.SnitchSubsystem, "size_exceeded").
		With(prometheus.Labels{"type": "count", "data_source": "rabbitmq"}).
		Add(float64(rand.Int63n(20)))
	counters.GetVecCounter(counters.SnitchSubsystem, "size_exceeded").
		With(prometheus.Labels{"type": "bytes", "data_source": "rabbitmq"}).
		Add(float64(rand.Int63n(10000)))

	fakeCounters(id1, ruleid1, "Name contains hello")
	a.PersistentConfig.RuleSets[id1] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:         id1,
			Name:       "Reject Messages",
			Mode:       common.RuleMode_RULE_MODE_PUBLISH,
			DataSource: "kafka",
			Version:    1,
			Key:        "mytopic",
			Rules: map[string]*common.Rule{
				ruleid1: {
					Id:   ruleid1,
					Name: "Name contains hello",
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.name",
							Type: "string_contains_any",
							Args: []string{"hello"},
						},
					},
					FailureModeConfigs: []*common.FailureMode{
						{
							Mode: common.RuleFailureMode_RULE_FAILURE_MODE_REJECT,
							Config: &common.FailureMode_Reject{
								Reject: &common.FailureModeReject{},
							},
						},
					},
				},
			},
		},
	}

	id2 := uuid.NewV4().String()
	ruleid2 := uuid.NewV4().String()
	fakeCounters(id2, ruleid2, "Address contains credit card")
	a.PersistentConfig.RuleSets[id2] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:         id2,
			Name:       "Slack Alert for Messages",
			Mode:       common.RuleMode_RULE_MODE_CONSUME,
			DataSource: "kafka",
			Version:    1,
			Key:        "mytopic",
			Rules: map[string]*common.Rule{
				ruleid2: {
					Id:   ruleid2,
					Name: "Address contains credit card",
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.address",
							Type: "pii_creditcard",
						},
					},
					FailureModeConfigs: []*common.FailureMode{
						{
							Mode: common.RuleFailureMode_RULE_FAILURE_MODE_ALERT_SLACK,
							Config: &common.FailureMode_AlertSlack{
								AlertSlack: &common.FailureModeAlertSlack{
									SlackChannel: "engineering",
								},
							},
						},
					},
				},
			},
		},
	}

	id3 := uuid.NewV4().String()
	ruleid3 := uuid.NewV4().String()
	fakeCounters(id3, ruleid3, "PII Credit Card")
	a.PersistentConfig.RuleSets[id3] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:         id3,
			Name:       "Messages to DLQ",
			Mode:       common.RuleMode_RULE_MODE_CONSUME,
			DataSource: "rabbitmq",
			Version:    1,
			Key:        "mytopic",
			Rules: map[string]*common.Rule{
				ruleid3: {
					Id:   ruleid3,
					Name: "PII Credit Card",
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.address",
							Type: "pii_creditcard",
						},
					},
					FailureModeConfigs: []*common.FailureMode{
						{
							Mode: common.RuleFailureMode_RULE_FAILURE_MODE_DLQ,
							Config: &common.FailureMode_Dlq{
								Dlq: &common.FailureModeDLQ{
									StreamdalToken: uuid.NewV4().String(),
								},
							},
						},
					},
				},
			},
		},
	}

	id4 := uuid.NewV4().String()
	ruleid4 := uuid.NewV4().String()
	fakeCounters(id4, ruleid4, "PII Credit Card 2")
	a.PersistentConfig.RuleSets[id4] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:         id4,
			Name:       "Transform message",
			Mode:       common.RuleMode_RULE_MODE_PUBLISH,
			DataSource: "rabbitmq",
			Version:    2,
			Rules: map[string]*common.Rule{
				ruleid4: {
					Id:   ruleid4,
					Name: "PII Credit Card 2",
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.ccnum",
							Type: "pii_creditcard",
						},
					},
					FailureModeConfigs: []*common.FailureMode{
						{
							Mode: common.RuleFailureMode_RULE_FAILURE_MODE_TRANSFORM,
							Config: &common.FailureMode_Transform{
								Transform: &common.FailureModeTransform{
									Path:  "payload.ccnum",
									Value: "****",
								},
							},
						},
					},
				},
			},
		},
	}

	WriteJSON(http.StatusOK, ResponseJSON{Message: "populated"}, w)
}

func fakeCounters(id1, ruleid1, name string) {
	counters.GetVecCounter(counters.SnitchSubsystem, "rule").
		With(prometheus.Labels{"rule_id": ruleid1, "ruleset_id": id1, "type": "count", "ruleset_name": "My ruleset", "rule_name": name}).
		Add(float64(rand.Int63n(10000)))
	counters.GetVecCounter(counters.SnitchSubsystem, "rule").
		With(prometheus.Labels{"rule_id": ruleid1, "ruleset_id": id1, "type": "bytes", "ruleset_name": "My ruleset", "rule_name": name}).
		Add(float64(rand.Int63n(100000)))

	counters.GetVecCounter(counters.SnitchSubsystem, "failure_trigger").
		With(prometheus.Labels{"rule_id": ruleid1, "ruleset_id": id1, "type": "count", "failure_mode": "discard", "ruleset_name": "My ruleset", "rule_name": name}).
		Add(float64(rand.Int63n(10000)))
	counters.GetVecCounter(counters.SnitchSubsystem, "failure_trigger").
		With(prometheus.Labels{"rule_id": ruleid1, "ruleset_id": id1, "type": "bytes", "failure_mode": "discard", "ruleset_name": "My ruleset", "rule_name": name}).
		Add(float64(rand.Int63n(100000)))
}

// marshalRules is needed because we're mixing go and protobuf types and can't use a single marshaller
func marshalRules(in map[string]*common.Rule) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}

	buf := bytes.NewBuffer([]byte(``))

	m := jsonpb.Marshaler{
		EnumsAsInts: false,
		OrigName:    true,
	}

	for k, v := range in {
		if err := m.Marshal(buf, v); err != nil {
			return nil, errors.Wrap(err, "could not marshal RuleSet")
		}
		out[k] = buf.Bytes()
	}

	return out, nil
}

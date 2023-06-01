package api

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
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
	rs.Rules = make(map[string]*common.Rule)

	a.PersistentConfig.SetRuleSet(id, &types.RuleSet{Set: rs})
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule set created", Values: map[string]string{"id": id}}, w)
}

func (a *API) updateRuleSetHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	rs := a.PersistentConfig.GetRuleSet(p.ByName("id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	update := &common.RuleSet{}

	if err := DecodeProtoBody(r.Body, update); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	rs.Set.Name = update.Name
	rs.Set.Mode = update.Mode
	rs.Set.Bus = update.Bus
	rs.Set.Version++

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule set updated"}, w)
}

func (a *API) deleteRuleSetHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	a.PersistentConfig.DeleteRuleSet(p.ByName("id"))

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule set deleted"}, w)
}

func (a *API) getRuleSetHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	set := a.PersistentConfig.GetRuleSet(p.ByName("id"))
	if set == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	WriteJSON(http.StatusOK, set.Set, w)
}

func (a *API) slackConfigHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	req := &SlackConfigRequest{}
	if err := DecodeBody(r.Body, req); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	a.PersistentConfig.SlackToken = req.Token
	a.PersistentConfig.Save()
}

func (a *API) getRulesHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	WriteJSON(http.StatusOK, rs.Set.Rules, w)
}

func (a *API) createRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
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

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule created", Values: map[string]string{"id": id}}, w)
}

func (a *API) updateRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
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

	a.PersistentConfig.SetRuleSet(rs.Set.Id, rs)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule updated"}, w)
}

func (a *API) deleteRuleHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	rs := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if rs == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	delete(rs.Set.Rules, p.ByName("id"))
	rs.Set.Version++

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
	a.PersistentConfig.RuleSets[id1] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      id1,
			Name:    "Reject Messages",
			Mode:    common.RuleMode_RULE_MODE_PUBLISH,
			Bus:     "kafka",
			Version: 1,
			Key:     "mytopic",
			Rules: map[string]*common.Rule{
				ruleid1: {
					Id:   ruleid1,
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.name",
							Type: "string_contains_any",
							Args: []string{"hello"},
						},
					},
					FailureMode:       common.RuleFailureMode_RULE_FAILURE_MODE_REJECT,
					FailureModeConfig: &common.Rule_Reject{},
				},
			},
		},
	}

	id2 := uuid.NewV4().String()
	ruleid2 := uuid.NewV4().String()
	a.PersistentConfig.RuleSets[id2] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      id2,
			Name:    "Slack Alert for Messages",
			Mode:    common.RuleMode_RULE_MODE_CONSUME,
			Bus:     "kafka",
			Version: 1,
			Key:     "mytopic",
			Rules: map[string]*common.Rule{
				ruleid2: {
					Id:   ruleid2,
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.address",
							Type: "pii_creditcard",
						},
					},
					FailureMode: common.RuleFailureMode_RULE_FAILURE_MODE_ALERT_SLACK,
					FailureModeConfig: &common.Rule_AlertSlack{
						AlertSlack: &common.FailureModeAlertSlack{
							SlackChannel: "engineering",
						},
					},
				},
			},
		},
	}

	id3 := uuid.NewV4().String()
	ruleid3 := uuid.NewV4().String()
	a.PersistentConfig.RuleSets[id3] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      id3,
			Name:    "Messages to DLQ",
			Mode:    common.RuleMode_RULE_MODE_CONSUME,
			Bus:     "rabbitmq",
			Version: 1,
			Key:     "mytopic",
			Rules: map[string]*common.Rule{
				ruleid3: {
					Id:   ruleid3,
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.address",
							Type: "pii_creditcard",
						},
					},
					FailureMode: common.RuleFailureMode_RULE_FAILURE_MODE_DLQ,
					FailureModeConfig: &common.Rule_Dlq{
						Dlq: &common.FailureModeDLQ{
							StreamdalToken: uuid.NewV4().String(),
						},
					},
				},
			},
		},
	}

	id4 := uuid.NewV4().String()
	ruleid4 := uuid.NewV4().String()
	a.PersistentConfig.RuleSets[id4] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      id4,
			Name:    "Transform message",
			Mode:    common.RuleMode_RULE_MODE_PUBLISH,
			Bus:     "rabbitmq",
			Version: 2,
			Rules: map[string]*common.Rule{
				ruleid4: {
					Id:   ruleid4,
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.ccnum",
							Type: "pii_creditcard",
						},
					},
					FailureMode: common.RuleFailureMode_RULE_FAILURE_MODE_TRANSFORM,
					FailureModeConfig: &common.Rule_Transform{
						Transform: &common.FailureModeTransform{
							Path:  "payload.ccnum",
							Value: "****",
						},
					},
				},
			},
		},
	}

	WriteJSON(http.StatusOK, ResponseJSON{Message: "populated"}, w)
}

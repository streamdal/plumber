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

	a.PersistentConfig.SetRuleSet(p.ByName("id"), rs)
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

	WriteJSON(http.StatusOK, set, w)
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
	set := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if set == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	WriteJSON(http.StatusOK, set.Set.Rules, w)
}

func (a *API) createRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	set := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if set == nil {
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

	if set.Set.Rules == nil {
		set.Set.Rules = make(map[string]*common.Rule)
	}
	set.Set.Rules[id] = rule

	a.PersistentConfig.SetRuleSet(p.ByName("id"), set)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule created", Values: map[string]string{"id": id}}, w)
}

func (a *API) updateRuleHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	set := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if set == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	rule := set.Set.Rules[p.ByName("id")]
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
	set.Set.Rules[rule.Id] = update
	set.Set.Version++

	a.PersistentConfig.SetRuleSet(set.Set.Id, set)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule updated"}, w)
}

func (a *API) deleteRuleHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	set := a.PersistentConfig.GetRuleSet(p.ByName("ruleset_id"))
	if set == nil {
		WriteJSON(http.StatusNotFound, ResponseJSON{Message: "rule set not found"}, w)
		return
	}

	delete(set.Set.Rules, p.ByName("id"))
	set.Set.Version++

	a.PersistentConfig.SetRuleSet(set.Set.Id, set)
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "rule deleted"}, w)
}

func (a *API) tempPopulateHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	a.PersistentConfig.RuleSetMutex.Lock()
	defer a.PersistentConfig.RuleSetMutex.Unlock()

	a.PersistentConfig.RuleSets = make(map[string]*types.RuleSet)

	a.PersistentConfig.RuleSets[uuid.NewV4().String()] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      uuid.NewV4().String(),
			Name:    "Reject Messages",
			Mode:    common.RuleMode_RULE_MODE_PUBLISH,
			Bus:     "kafka",
			Version: 1,
			Key:     "mytopic",
			Rules: map[string]*common.Rule{
				uuid.NewV4().String(): {
					Id:   uuid.NewV4().String(),
					Type: common.RuleType_RULE_TYPE_MATCH,
					RuleConfig: &common.Rule_MatchConfig{
						MatchConfig: &common.RuleConfigMatch{
							Path: "payload.name",
							Type: "string_contains",
							Args: []string{"hello"},
						},
					},
					FailureMode:       common.RuleFailureMode_RULE_FAILURE_MODE_REJECT,
					FailureModeConfig: &common.Rule_Reject{},
				},
			},
		},
	}

	a.PersistentConfig.RuleSets[uuid.NewV4().String()] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      uuid.NewV4().String(),
			Name:    "Slack Alert for Messages",
			Mode:    common.RuleMode_RULE_MODE_CONSUME,
			Bus:     "kafka",
			Version: 1,
			Key:     "mytopic",
			Rules: map[string]*common.Rule{
				uuid.NewV4().String(): {
					Id:   uuid.NewV4().String(),
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

	a.PersistentConfig.RuleSets[uuid.NewV4().String()] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      uuid.NewV4().String(),
			Name:    "Messages to DLQ",
			Mode:    common.RuleMode_RULE_MODE_CONSUME,
			Bus:     "rabbitmq",
			Version: 1,
			Key:     "mytopic",
			Rules: map[string]*common.Rule{
				uuid.NewV4().String(): {
					Id:   uuid.NewV4().String(),
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

	a.PersistentConfig.RuleSets[uuid.NewV4().String()] = &types.RuleSet{
		Set: &common.RuleSet{
			Id:      uuid.NewV4().String(),
			Name:    "Transform message",
			Mode:    common.RuleMode_RULE_MODE_PUBLISH,
			Bus:     "rabbitmq",
			Version: 2,
			Rules: map[string]*common.Rule{
				uuid.NewV4().String(): {
					Id:   uuid.NewV4().String(),
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

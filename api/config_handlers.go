package api

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (a *API) getSlackConfigHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	WriteJSON(http.StatusOK, ResponseJSON{Values: map[string]string{"token": a.PersistentConfig.SlackToken}}, w)
}

func (a *API) updateSlackConfigHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	req := &SlackConfigRequest{}
	if err := DecodeBody(r.Body, req); err != nil {
		WriteJSON(http.StatusBadRequest, ResponseJSON{Message: err.Error()}, w)
		return
	}

	a.PersistentConfig.SlackToken = req.Token
	a.PersistentConfig.Save()

	WriteJSON(http.StatusOK, ResponseJSON{Message: "slack config updated"}, w)
}
